import os from 'os'
import fs from 'fs'
import { join } from 'path'
import initDebug from 'debug'

const debug = initDebug('s3-tus-store')

export default async (prefix = 's3-tus-store') => {
  debug('create tmp file')
  const dir = await new Promise((resolve, reject) => {
    fs.mkdtemp(join(os.tmpDir(), prefix), (err, result) => {
      if (err) return reject(err)
      resolve(result)
    })
  })
  const path = join(dir, 'tmpfile')
  const createWriteStream = () => fs.createWriteStream(path)
  const createReadStream = () => fs.createReadStream(path)

  const rm = async () => new Promise((resolve, reject) => {
    fs.unlink(path, (err) => {
      if (err) return reject(err)
      fs.rmdir(dir, (err2) => {
        if (err2) return reject(err2)
        resolve()
      })
    })
  })

  return {
    path,
    createWriteStream,
    createReadStream,
    rm,
  }
}
