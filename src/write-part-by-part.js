import streamSplitter from 'fixed-size-stream-splitter'
import initDebug from 'debug'
import { SizeStream } from 'common-streams'
import { PassThrough } from 'stream'

import createTmpFile from './tmp-file'

const debug = initDebug('s3-tus-store')

const uploadPart = async (rs, guessedPartSize, partNumber, {
  client,
  bucket,
  uploadId,
  minPartSize,
  key,
}) => {
  //
  // Optimistically guess that Content-Length is guessedPartSize
  // but keep a temporary copy on disk in case the stream ends
  // before we reached have "guessedPartSize"
  //
  // If the actual part size is > minPartSize, write part with
  // content-length = actual part size
  //
  // Otherwise, there is not much we can do, short of temporarily
  // writing the data to S3 key and waiting for the next call to
  // to .append() to read it, merge it with the new stream
  // and write to a new part.... But that is a TODO!
  //

  const through = rs.pipe(new PassThrough())
  const Body = new PassThrough()

  const baseParams = {
    Key: key,
    Bucket: bucket,
    UploadId: uploadId,
    PartNumber: partNumber,
  }

  const request = client.uploadPart({
    Body,
    ...baseParams,
    ContentLength: guessedPartSize,
  })

  // Ensure body is not smaller than content length,
  // otherwise requests stall indefinitely.
  const tmpFile = await createTmpFile()
  debug(tmpFile.path)

  const fileWrittenPromise = new Promise((resolve, reject) => {
    through
      .pipe(tmpFile.createWriteStream())
      .on('error', (err) => reject(err))
      .on('finish', () => resolve())
  })
  const streamSizePromise = new Promise((resolve) => {
    through
      .pipe(new SizeStream((byteCount) => {
        resolve(byteCount)
      }))
      .pipe(Body)
  })

  let actualSize
  streamSizePromise.then((size) => {
    actualSize = size
    if (size < guessedPartSize) {
      request.abort()
    }
  })
  // stream was shorter than we expected,
  // we aborted the request. now, let's make sure
  // tmp file is written and try to upload its content
  // with correct content length
  const planB = async () => {
    // Nothing we can do.. short of uploading to a S3 key...
    // and rewriting later when we have a new write? TODO
    debug('plan B')
    debug(`actualSize = ${actualSize}`)
    debug(`minPartSize = ${minPartSize}`)
    if (actualSize < minPartSize) {
      tmpFile.rm() // dont need wait for this...
      return
    }
    // make sure file completely written to disk...
    await fileWrittenPromise
    const { ETag } = await client.uploadPart({
      ...baseParams,
      Body: tmpFile.createReadStream(),
      ContentLength: actualSize,
    }).promise()

    tmpFile.rm() // dont need wait for this

    // TODO: put whole function in try/catch to make sure tmpfile
    // remove even when errors...
    return {
      ETag,
      PartNumber: partNumber,
      Size: actualSize,
    }
  }

  const planA = () => request
    .promise()
    .then(({ ETag }) => ({
      ETag,
      PartNumber: partNumber,
      Size: guessedPartSize,
    }))
    .catch((err) => {
      if (err.code === 'RequestAbortedError') {
        debug('request aborted')
        return planB()
      }
      throw err
    })
    /*
    .then((result) => {
      debug(result)
      return result
    })
    */
  return planA()
}

export default (opts = {}) => new Promise((resolve, reject) => {
  const {
    body,
    maxPartSize,
    bytesLimit,
    nextPartNumber,
  } = opts
  let done = false
  let promise = Promise.resolve()

  let splitIndex = 0
  const newParts = []
  const onSplit = (rs) => {
    if (done) return
    const partNumber = nextPartNumber + splitIndex
    const bytesWritten = splitIndex * maxPartSize
    const bytesRemaining = bytesLimit - bytesWritten
    const guessedPartSize = Math.min(bytesRemaining, maxPartSize)
    // wait for previous uploadPart operation to complete
    promise = promise
      .then(() => (
        uploadPart(rs, guessedPartSize, partNumber, opts)
      ))
      .then(newPart => {
        newParts.push(newPart)
      })
      .catch((err) => {
        done = true
        reject(err)
      })
    splitIndex += 1
  }

  body
    .on('error', (err) => {
      reject(err)
    })
    .pipe(streamSplitter(maxPartSize, onSplit))
    .on('finish', () => {
      if (done) return
      // Make sure all upload part promises are completed...
      promise.then(() => { resolve(newParts) }).catch(reject)
    })
})
