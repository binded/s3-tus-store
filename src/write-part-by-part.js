import streamSplitter from 'fixed-size-stream-splitter'
/*
const writePart = (client, key, uploadId, partNumber, rs) => {
  client.uploadPart(buildParams(key, {
    UploadId: uploadId,
    PartNumber: partNumber,
    Body: body,
  }), (err) => {

    if (done) return
      done = true
        if (err) return reject(err)
          if (bytesRead < minPartSize) {
            return reject(
                new Error(`part was ${bytesRead} bytes, min part size is ${minPartSize} bytes`)
                )
          }
    resolve()
  })
}
*/
const initUploadPart = (client, bucket, key, uploadId, start) => {
  let index = 0

  return (body) => {
    const partNumber = start + index
    index += 1
    return client.uploadPart({
      Bucket: bucket,
      Key: key,
      UploadId: uploadId,
      PartNumber: partNumber,
      Body: body,
    }).promise()
  }
}

export default (
  body,
  client,
  bucket,
  key,
  uploadId,
  partNumber,
  maxPartSize,
) => new Promise((resolve, reject) => {
  const uploadPart = initUploadPart(client, bucket, key, uploadId, partNumber)
  let promise = Promise.resolve()
  let done = false
  body
    .pipe(streamSplitter(maxPartSize, (rs) => {
      promise = promise.then(() => uploadPart(rs))
        .catch((err) => {
          done = true
          reject(err)
        })
    }))
    .on('end', () => {
      if (done) return
      // Make sure all upload part promises are completed...
      promise.then(() => { resolve() }).catch(reject)
    })
})
