import initDebug from 'debug'
import toObject from 'to-object-reducer'
import MeterStream from 'meterstream'
import { PassThrough } from 'stream'
import { SizeStream } from 'common-streams'
import { errors } from 'abstract-tus-store'
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
  maxPartSize: 6 * 1024 * 1024, // 6 MB
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
  maxPartSize = defaults.maxPartSize,
}) => {
  const buildParams = (key, extra) => ({
    Key: key,
    Bucket: bucket,
    ...extra,
  })

  const buildS3Metadata = (uploadMetadata = {}) => {
    const metadata = uploadMetadata
    // Values must be strings... :(
    // TODO: test what happens with non ASCII keys/values
    return Object
      .keys(metadata)
      .map(key => ([key, `${metadata[key]}`]))
      .reduce(toObject, {})
  }

  const getUploadKey = (uploadId) => `tus-uploads/${uploadId}`
  const getUpload = async (uploadId) => {
    const { Body } = await client
      .getObject(buildParams(getUploadKey(uploadId)))
      .promise()
      .catch((err) => {
        if (err.code === 'NoSuchUpload') {
          throw new errors.UploadNotFound(uploadId)
        }
        throw err
      })
    return JSON.parse(Body)
  }

  const saveUpload = async (uploadId, upload) => {
    const key = getUploadKey(uploadId)
    const json = JSON.stringify(upload)
    await client.putObject(buildParams(key, {
      Body: json,
      ContentLength: json.length,
    })).promise()
  }

  const getUploadOffset = async (uploadId) => {
    const { Parts } = await client.listParts(buildParams(null, {
      UploadId: uploadId,
    })).promise()
    // sum size of all parts
    return Parts
      .map(({ Size }) => Size)
      .reduce((total, size) => total + size, 0)
  }

  const create = async (key, {
    uploadLength,
    metadata = {},
  }) => {
    const { UploadId } = await client.createMultipartUpload(buildParams(key, {
      Metadata: buildS3Metadata(metadata),
    })).promise()
    const uploadId = UploadId
    const upload = {
      key,
      uploadLength,
      metadata,
    }
    await saveUpload(uploadId, upload)
    return { uploadId }
  }

  const info = async uploadId => {
    const upload = await getUpload(uploadId)
    const offset = await getUploadOffset(uploadId)
    return {
      offset,
      ...upload,
    }
  }

  const append = async (key, rs) => {
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
    const bytesRemaining = uploadLength - uploadOffset
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

      // Make sure length is right:
      const sizeOfParts = Parts.map(({ Size }) => Size).reduce((a, b) => a + b, 0)
      if (sizeOfParts !== uploadLength) {
        throw new Error(
          `size of part mismatch, expected ${uploadLength} bytes but got ${sizeOfParts} bytes`
        )
      }

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

  /*
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
    const bytesRemaining = uploadLength - uploadOffset
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

      // Make sure length is right:
      const sizeOfParts = Parts.map(({ Size }) => Size).reduce((a, b) => a + b, 0)
      if (sizeOfParts !== uploadLength) {
        throw new Error(
          `size of part mismatch, expected ${uploadLength} bytes but got ${sizeOfParts} bytes`
        )
      }

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
  */

  const createReadStream = key => client
    .getObject(buildParams(key))
    .createReadStream()

  return {
    info,
    create,
    append,
    createReadStream,
    minChunkSize: minPartSize,
  }
}
