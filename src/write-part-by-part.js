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

export default (client, body, key, uploadId, partNumber, minPartSize, maxPartSize) => {

}
