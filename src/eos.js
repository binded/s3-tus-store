// Promisified version of end-of-stream
import eos from 'end-of-stream'

export default (stream, opts = {}) => new Promise((resolve, reject) => {
  eos(stream, opts, err => {
    if (err) return reject(err)
    resolve()
  })
})
