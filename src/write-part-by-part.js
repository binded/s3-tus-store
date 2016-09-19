import streamSplitter from 'fixed-size-stream-splitter'
import initDebug from 'debug'
import { SizeStream } from 'common-streams'
import { PassThrough } from 'stream'

const debug = initDebug('s3-tus-store')

const calcContentLength = (index, partSize, bytesRemaining) => {
  const currentBytesRemaining = bytesRemaining - (index * partSize)
  return Math.min(partSize, currentBytesRemaining)
}

const initUploadPart = ({
  client,
  bucket,
  key,
  uploadId,
  nextPartNumber,
  partSize,
  bytesRemaining,
}) => {
  let index = 0
  return (body) => {
    const partNumber = nextPartNumber + index
    const contentLength = calcContentLength(index, partSize, bytesRemaining)
    index += 1

    const through = new PassThrough()

    const params = {
      Bucket: bucket,
      Key: key,
      UploadId: uploadId,
      PartNumber: partNumber,
      Body: through,
      ContentLength: contentLength,
    }
    debug({ ...params, Body: null })
    const request = client.uploadPart(params)

    // Ensure body is not smaller than content length,
    // otherwise requests stall indefinitely.
    let bodySize
    body
      .pipe(new SizeStream((byteCount) => {
        bodySize = byteCount
        if (byteCount < contentLength) {
          request.abort()
        }
      }))
      .pipe(through)

    return request
      .promise()
      /*
      .then((response) => {
        debug(response)
        return response
      })
      */
      .then(({ ETag }) => ({
        ETag,
        PartNumber: partNumber,
        Size: contentLength,
      }))
      .catch((err) => {
        if (err.code === 'RequestAbortedError') {
          throw new Error(`body (${bodySize}) smaller than content length (${contentLength})`)
        }
        throw err
      })
  }
}

export default (opts = {}) => new Promise((resolve, reject) => {
  const uploadPart = initUploadPart(opts)
  const { body, partSize } = opts
  let promise = Promise.resolve()
  let done = false
  const newParts = []
  body
    .pipe(streamSplitter(partSize, (rs) => {
      promise = promise.then(() => uploadPart(rs))
        .then(newPart => {
          newParts.push(newPart)
        })
        .catch((err) => {
          done = true
          reject(err)
        })
    }))
    .on('finish', () => {
      if (done) return
      // Make sure all upload part promises are completed...
      promise.then(() => { resolve(newParts) }).catch(reject)
    })
})
