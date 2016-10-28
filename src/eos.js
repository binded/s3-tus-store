// Promisified version of end-of-stream
import eos from 'end-of-stream'

export default (stream) => new Promise((resolve, reject) => {
  eos(stream, err => {
    if (err) return reject(err)
    resolve()
  })
})
