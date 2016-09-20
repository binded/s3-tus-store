import initDebug from 'debug'
import toObject from 'to-object-reducer'
import MeterStream from 'meterstream'
import { PassThrough } from 'stream'
import { SizeStream } from 'common-streams'
// import { inspect } from 'util'

import writePartByPart from './write-part-by-part'

// Inspired by https://github.com/tus/tusd/blob/master/s3store/s3store.go
//
// Configuration
//
// In order to allow this backend to function properly, the user accessing the
// bucket must have at least following AWS IAM policy permissions for the
// bucket and all of its subresources:
// 	s3:AbortMultipartUpload
// 	s3:DeleteObject
// 	s3:GetObject
// 	s3:ListMultipartUploadParts
// 	s3:PutObject//
// Implementation
//
// Once a new tus upload is initiated, multiple objects in S3 are created:
//
// First of all, a new info object is stored which contains a JSON-encoded blob
// of general information about the upload including its size and meta data.
// This kind of objects have the suffix ".info" in their key.
//
// In addition a new multipart upload
// (http://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html) is
// created. Whenever a new chunk is uploaded to tusd using a PATCH request, a
// new part is pushed to the multipart upload on S3.
//
// If meta data is associated with the upload during creation, it will be added
// to the multipart upload and after finishing it, the meta data will be passed
// to the final object. However, the metadata which will be attached to the
// final object can only contain ASCII characters and every non-ASCII character
// will be replaced by a question mark (for example, "Menü" will be "Men?").
// However, this does not apply for the metadata returned by the GetInfo
// function since it relies on the info object for reading the metadata.
// Therefore, HEAD responses will always contain the unchanged metadata, Base64-
// encoded, even if it contains non-ASCII characters.

const debug = initDebug('s3-tus-store')

const defaults = {
	// MaxPartSize specifies the maximum size of a single part uploaded to S3
	// in bytes. This value must be bigger than minPartSize! In order to
	// choose the correct number, two things have to be kept in mind:
	//
	// If this value is too big and uploading the part to S3 is interrupted
	// unexpectedly, the entire part is discarded and the end user is required
	// to resume the upload and re-upload the entire big part.
	//
	// If this value is too low, a lot of requests to S3 may be made, depending
	// on how fast data is coming in. This may result in an eventual overhead.
  // maxPartSize: 6 * 1024 * 1024, // 6 MB
  //
  // @oli we set maxPartSize to minPartSize so we dont need
  // to know content length in advance
  //
	// MinPartSize specifies the minimum size of a single part uploaded to S3
	// in bytes. This number needs to match with the underlying S3 backend or else
	// uploaded parts will be reject. AWS S3, for example, uses 5MB for this value.
  minPartSize: 5 * 1024 * 1024,
}

// TODO: optional TTL?
export default ({
  client,
  bucket,
  minPartSize = defaults.minPartSize,
  // maxPartSize = defaults.maxPartSize,
}) => {
  const buildParams = (key, extra) => ({
    Key: key,
    Bucket: bucket,
    ...extra,
  })

  const infoKey = (key) => `${key}.info`

  const buildS3Metadata = (uploadMetadata = {}) => {
    const metadata = uploadMetadata
    // Values must be strings... :(
    // TODO: test what happens with non ASCII keys/values
    return Object
      .keys(metadata)
      .map(key => ([key, `${metadata[key]}`]))
      .reduce(toObject, {})
  }

  const setKeyInfo = async (key, info) => {
    const infoJson = JSON.stringify(info)
    return await client
      .putObject(buildParams(infoKey(key), {
        Body: infoJson,
        ContentLength: infoJson.length,
      }))
      .promise()
  }

  const getKeyInfo = async key => {
    const { Body } = await client
      .getObject(buildParams(infoKey(key)))
      .promise()
    const info = JSON.parse(Body.toString())
    return info
  }

  const getKeyOffset = async (key, uploadId) => {
    const { Parts } = await client
      .listParts(buildParams(key, {
        UploadId: uploadId,
      }))
      .promise()
    // get size of all parts
    return Parts
      .map(({ Size }) => Size)
      // sum size of all parts
      .reduce((total, size) => total + size, 0)
  }

  const info = async key => {
    const infoObj = await getKeyInfo(key)
    const uploadOffset = await getKeyOffset(key, infoObj.uploadId)
      .catch(err => {
        // we should only get that error when an upload is completed
        if (err.code === 'NoSuchUpload') {
          return infoObj.uploadLength
        }
        throw err
      })
    debug(`uploadOffset is ${uploadOffset}`)
    return {
      ...infoObj,
      uploadOffset,
    }
  }

  // TODO: make sure not already created?
  // TODO: save uploadMetadata in a JSON string?
  // in case it might contain a uploadLength key...
  const create = async (key, {
    uploadLength,
    uploadMetadata = {},
  }) => {
    const { UploadId } = await client.createMultipartUpload(buildParams(key, {
      Metadata: buildS3Metadata(uploadMetadata),
    })).promise()
    const uploadId = UploadId
    const infoObj = { uploadId, uploadLength, uploadMetadata }
    return setKeyInfo(key, infoObj)
  }

  const getWriteInfo = async (key) => {
    const { uploadId, uploadOffset, uploadLength } = await info(key)
    const { Parts } = await client
      .listParts(buildParams(key, {
        UploadId: uploadId,
      }))
      .promise()

    const parts = Parts

    // parts are 1-indexed
    let nextPartNumber
    if (!parts.length) {
      nextPartNumber = 1
    } else {
      const lastPart = parts[parts.length - 1]
      nextPartNumber = lastPart.PartNumber + 1
    }

    return {
      uploadId,
      uploadOffset,
      uploadLength,
      nextPartNumber,
      parts,
    }
  }

  const write = async (key, rs) => {
    // need to do this asap to make sure we don't miss reads
    const readStream = rs.pipe(new PassThrough())
    const {
      uploadId,
      uploadOffset,
      uploadLength,
      nextPartNumber,
      parts,
    } = await getWriteInfo(key)

    // TODO: only do this if uploadLength is set
    debug('minPartSize', minPartSize)
    debug('uploadLength', uploadLength)
    const bytesRemaining = uploadLength - uploadOffset
    debug('bytesRemaining', bytesRemaining)
    // Ensure total upload doesn't exeedd uploadLength
    const meter = new MeterStream(bytesRemaining)
    let bytesUploaded
    // Count how many bytes have been uploaded
    const sizeStream = new SizeStream((byteCount) => {
      bytesUploaded = byteCount
    })
    const body = new PassThrough()

    const newParts = await new Promise((resolve, reject) => {
      let done = false
      // This should only happen with a "malicious" client
      meter.on('error', (err) => {
        done = true
        // TODO: make sure we need to call .end() on body
        body.end()
        reject(err)
      })
      // Splits body into multiple consecutive part uploads
      writePartByPart({
        body,
        client,
        bucket,
        key,
        uploadId,
        nextPartNumber,
        partSize: minPartSize,
        bytesRemaining,
      })
        .then((result) => {
          if (done) return
          resolve(result)
        }, (err) => {
          if (done) return
          reject(err)
        })
      readStream.pipe(meter).pipe(sizeStream).pipe(body)
    })

    debug(`new parts ${newParts}`)
    debug(`uploaded ${bytesUploaded} bytes`)
    // Upload completed!
    debug(`uploadLength is ${uploadLength}`)
    debug(`bytesUploaded is ${bytesUploaded}`)

    if (uploadOffset + bytesUploaded === uploadLength) {
      debug('Completing upload!')
      const Parts = [
        ...parts,
        ...newParts,
      ]
      // console.log(Parts)
      // Make sure length is right:
      const sizeOfParts = Parts.map(({ Size }) => Size).reduce((a, b) => a + b, 0)
      if (sizeOfParts !== uploadLength) {
        throw new Error(
          `size of part mismatch, expected ${uploadLength} bytes but got ${sizeOfParts} bytes`
        )
      }

      // Last part less than minPartSize implies that S3 auto completed
      // the upload
      /*
      if (Parts[Parts.length - 1].Size < minPartSize) {
        return
      }
      */
      const preparePartForParams = ({ ETag, PartNumber }) => ({ ETag, PartNumber })
      const MultipartUpload = {
        Parts: Parts.map(preparePartForParams),
      }
      const completeUploadParams = buildParams(key, {
        MultipartUpload,
        UploadId: uploadId,
      })
      debug(completeUploadParams.MultipartUpload)
      return client.completeMultipartUpload(completeUploadParams)
        .promise()
    } else if (bytesUploaded < minPartSize) {
      throw new Error(
        `Uploaded ${bytesUploaded} bytes but minPartSize is ${minPartSize} bytes`
      )
    } else {
      debug('upload not completed yet')
    }
  }

  const createReadStream = key => client.getObject(buildParams(key)).createReadStream()

  return {
    info,
    create,
    write,
    createReadStream,
    minPartSize,
  }
}
