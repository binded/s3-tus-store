import streamSplitter from 'fixed-size-stream-splitter'

const calcContentLength = (size, index, maxPartSize) => {
  if (typeof size === 'undefined') return
  const bytesRemaining = size - (index * maxPartSize)
  return Math.min(bytesRemaining, maxPartSize)
}

const initUploadPart = (size, client, bucket, key, uploadId, start, maxPartSize) => {
  let index = 0
  return (body) => {
    const partNumber = start + index
    const contentLength = calcContentLength(size, index, maxPartSize)

    index += 1
    return client.uploadPart({
      Bucket: bucket,
      Key: key,
      UploadId: uploadId,
      PartNumber: partNumber,
      Body: body,
      ContentLength: contentLength,
    }).promise().then(({ ETag }) => ({ ETag, PartNumber: partNumber }))
  }
}

export default (
  body,
  size,
  client,
  bucket,
  key,
  uploadId,
  partNumber,
  maxPartSize,
) => new Promise((resolve, reject) => {
  const uploadPart = initUploadPart(size, client, bucket, key, uploadId, partNumber, maxPartSize)
  let promise = Promise.resolve()
  let done = false
  const newParts = []
  body
    .pipe(streamSplitter(maxPartSize, (rs) => {
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
