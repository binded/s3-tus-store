import initDebug from 'debug'
import toObject from 'to-object-reducer'
import MeterStream from 'meterstream'
import { PassThrough } from 'stream'
import duplexify from 'duplexify'
// import { SizeStream } from 'common-streams'
import { errors } from 'abstract-tus-store'
// import { inspect } from 'util'

import eos from './eos'
import writePartByPart from './write-part-by-part'

// TODO: docs below slightly outdated!
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
// TODO: MAKE SURE UPLOADID IS UNIQUE REGARDLESS OF KEY
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
    debug('buildS3Metadata')
    const metadata = uploadMetadata
    // Values must be strings... :(
    // TODO: test what happens with non ASCII keys/values
    const validMetadata = Object
      .keys(metadata)
      .map(key => ([key, `${metadata[key]}`]))
      // strip non US ASCII characters
      .map(([key, str]) => [key, Buffer.from(str, 'ascii').toString('ascii')])
      .reduce(toObject, {})
    return validMetadata
  }

  const getUploadKey = (uploadId) => `tus-uploads/${uploadId}`
  const getUploadKeyForKey = (key) => `tus-uploads/finished/${key}.upload`

  const getUploadForKey = async (key) => {
    debug('buildS3Metadata', { key })
    const { Body } = await client
      .getObject(buildParams(getUploadKeyForKey(key)))
      .promise()
    return JSON.parse(Body)
  }

  const getUpload = async (uploadId) => {
    debug('getUpload', { uploadId })
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
    debug('saveUpload', { uploadId, upload })
    const key = getUploadKey(uploadId)
    const json = JSON.stringify(upload)
    await client.putObject(buildParams(key, {
      Body: json,
      ContentLength: Buffer.byteLength(json),
    })).promise()
  }

  const saveUploadForKey = async (uploadId, upload) => {
    debug('saveUploadForKey', { uploadId, upload })
    const key = getUploadKeyForKey(upload.key)
    const json = JSON.stringify({
      ...upload,
      uploadId,
    })
    await client.putObject(buildParams(key, {
      Body: json,
      ContentLength: Buffer.byteLength(json),
    })).promise()
  }

  const getParts = async (uploadId, key) => {
    const { Parts = [] } = await client.listParts(buildParams(key, {
      UploadId: uploadId,
    })).promise()
    debug('getParts', Parts)
    return Parts
  }

  const countSizeFromParts = (parts) => parts
    .map(({ Size }) => Size)
    .reduce((total, size) => total + size, 0)

  const getUploadOffset = async (uploadIdOrParts, key) => {
    debug('getUploadOffset', { uploadIdOrParts, key })
    if (Array.isArray(uploadIdOrParts)) {
      debug('parts', uploadIdOrParts)
      return countSizeFromParts(uploadIdOrParts)
    }
    const parts = await getParts(uploadIdOrParts, key)
    if (!Array.isArray(parts)) {
      throw new Error('this should never happen')
    }
    return getUploadOffset(parts)
  }

  const create = async (key, {
    uploadLength,
    metadata = {},
  }) => {
    debug('create', { key })
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
    debug('info', { uploadId })
    const upload = await getUpload(uploadId)
    debug(upload)
    const offset = await getUploadOffset(uploadId, upload.key)
      .then(uploadOffset => {
        if (uploadOffset === upload.uploadLength) {
          // upload is completed but completeMultipartUpload has not been
          // called for some reason...
          // force a last call to append :/
          return uploadOffset - 1
        }
        return uploadOffset
      })
      .catch(err => {
        // we got the upload file but upload part does not exist
        // that means the upload is actually completed.
        if (err.code === 'NoSuchUpload') {
          return upload.uploadLength
        }
        throw err
      })
    return {
      offset,
      ...upload,
    }
  }

  const createLimitStream = (uploadLength, offset) => {
    if (typeof uploadLength === 'undefined') {
      return new PassThrough()
    }
    const meterStream = new MeterStream(uploadLength - offset)
    return meterStream
  }

  const afterWrite = async (
      uploadId,
      upload,
      beforeComplete,
      parts,
    ) => {
    const offset = await getUploadOffset(parts)
    // Upload complete!
    debug(`offset = ${offset}`)
    // TODO: what happens if process crashes here? multipart upload never completes...
    // So, when we get HEAD (info()) request and the offset === uploadLength, we have to
    // check that the multipart upload was really completed and if not, complete it before
    // returning a response
    if (offset === upload.uploadLength) {
      debug('Completing upload!')
      await beforeComplete(upload, uploadId)
      const MultipartUpload = {
        Parts: parts.map(({ ETag, PartNumber }) => ({ ETag, PartNumber })),
      }
      const completeUploadParams = buildParams(null, {
        MultipartUpload,
        UploadId: uploadId,
        Key: upload.key,
      })
      await saveUploadForKey(uploadId, upload)
      debug('completeUpload', { completeUploadParams })
      await client
        .completeMultipartUpload(completeUploadParams)
        .promise()
      // TODO: remove upload file?
      return {
        offset,
        complete: true,
        upload: {
          ...upload,
          offset,
        },
      }
    }
    return { offset }
  }

  const append = async (uploadId, rs, arg3, arg4) => {
    // guess arg by type
    const { expectedOffset, opts = {} } = (() => {
      if (typeof arg3 === 'object') {
        return { opts: arg3 }
      }
      return { expectedOffset: arg3, opts: arg4 }
    })()
    debug('append', { uploadId, expectedOffset })
    const { beforeComplete = async () => {} } = opts

    // need to do this asap to make sure we don't miss reads
    const through = rs.pipe(new PassThrough())
    const rsEos = eos(rs)

    debug('append opts', opts)

    const upload = await getUpload(uploadId)
    const parts = await getParts(uploadId, upload.key)
    const offset = await getUploadOffset(parts)

    // For some reason, upload is finished but not completed yet
    if (offset === upload.uploadLength) {
      if (!Number.isInteger(expectedOffset) || expectedOffset === upload.uploadLength - 1) {
        return afterWrite(uploadId, upload, beforeComplete, parts)
      }
    }

    if (Number.isInteger(expectedOffset)) {
      // check if offset is right
      if (offset !== expectedOffset) {
        throw new errors.OffsetMismatch(offset, expectedOffset)
      }
    }

    const limitStream = createLimitStream(upload.uploadLength, offset)
    const limitStreamEos = eos(limitStream)

    // Parts are 1-indexed
    const nextPartNumber = parts.length
      ? parts[parts.length - 1].PartNumber + 1
      : 1

    const bytesLimit = Number.isInteger(upload.uploadLength) ?
      upload.uploadLength - offset : Infinity

    const newParts = await writePartByPart({
      client,
      bucket,
      uploadId,
      nextPartNumber,
      maxPartSize,
      minPartSize,
      bytesLimit,
      key: upload.key,
      body: through.pipe(limitStream), // .pipe(sizeStream),
    })

    // This ensures that if either stream emitted an error,
    // our promise will throw
    await Promise.all([rsEos, limitStreamEos])

    return afterWrite(uploadId, upload, beforeComplete, [
      ...parts,
      ...newParts,
    ])
  }

  const createReadStream = (key, onInfo) => {
    debug('createReadStream', { key })
    const rs = duplexify()
    const letsgo = async () => {
      const body = client
        .getObject(buildParams(key))
        .createReadStream()
      rs.setReadable(body)
      // https://github.com/aws/aws-sdk-js/issues/1153
      if (onInfo) {
        debug('onInfo', { key })
        // const data = await client.headObject(buildParams(key)).promise()
        // S3 metadata sucks...
        const { uploadLength, metadata } = await getUploadForKey(key)
        onInfo({
          metadata,
          contentLength: uploadLength,
        })
      }
    }
    letsgo()
    return rs
  }

  return {
    info,
    create,
    append,
    createReadStream,
    minChunkSize: minPartSize,
  }
}
