import streamSplitter from 'fixed-size-stream-splitter'
import initDebug from 'debug'
import { SizeStream } from 'common-streams'
import { PassThrough } from 'stream'

import eos from './eos'
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
  debug('tmpFile path', tmpFile.path)

  // In parallel, we write to a temporary file to resume
  // in case original stream fails (we guessed part size wrong)
  const fileWrittenPromise = eos(
    through.pipe(tmpFile.createWriteStream())
  )

  const streamSizePromise = new Promise((resolve) => {
    through
      .pipe(new SizeStream((byteCount) => {
        // This will resolve once the upload to S3 is done
        resolve(byteCount)
      }))
      // Upload to S3 hasn't started yet
      .pipe(Body)
  })

  let actualSize
  // Wait for upload to complete
  streamSizePromise.then((size) => {
    actualSize = size
    if (size < guessedPartSize) {
      debug('Oops, our guessedPartSize was larger than actualSize')
      // make sure request is aborted
      request.abort()
    }
  })
  // stream was shorter than we expected,
  // we aborted the request. now, let's make sure
  // tmp file is written and try to upload its content
  // with correct content length
  const planB = async () => {
    debug('plan B')
    debug(`actualSize = ${actualSize}`)
    debug(`minPartSize = ${minPartSize}`)
    if (actualSize < minPartSize) {
      // Nothing we can do.. short of uploading to a S3 key...
      // and rewriting later when we have a new write? TODO
      // PS: we always guess the size of the last part correctly
      // so this is never called for the last part
      tmpFile.rm() // dont need wait for this...
      throw new Error(`Upload parts must be at least ${minPartSize}`)
    }
    // make sure temporary was file completely written to disk...
    await fileWrittenPromise
    const tmpFileRs = tmpFile.createReadStream()

    // Captures errors so we dont get uncaught error events
    const tmpFileRsEos = eos(tmpFileRs)

    const { ETag } = await client.uploadPart({
      ...baseParams,
      Body: tmpFileRs,
      ContentLength: actualSize,
    }).promise()

    tmpFile.rm() // dont need wait for this

    // TODO: put whole function in try/catch to make sure tmpfile
    // remove even when errors...

    // Make sure tmp file read stream didn't emit an error
    await tmpFileRsEos

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
    // We always guess the size of the last part correctly
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
    .on('error', reject)
    .on('finish', () => {
      if (done) return
      // Make sure all upload part promises are completed...
      promise.then(() => { resolve(newParts) }).catch(reject)
    })
})
