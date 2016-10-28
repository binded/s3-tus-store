import os from 'os'
import fs from 'fs'
import { join } from 'path'
import initDebug from 'debug'

const debug = initDebug('s3-tus-store:tmpfile')

export default async (prefix = 's3-tus-store') => {
  const tmpDirPath = join(os.tmpDir(), prefix)
  debug('create tmp dir', { tmpDirPath })
  const dir = await new Promise((resolve, reject) => {
    fs.mkdtemp(tmpDirPath, (err, result) => {
      if (err) return reject(err)
      resolve(result)
    })
  })
  const path = join(dir, 'tmpfile')
  const createWriteStream = () => {
    debug('createWriteStream', { path })
    return fs.createWriteStream(path)
  }
  const createReadStream = () => {
    debug('createReadStream', { path })
    return fs.createReadStream(path)
  }

  const rm = async () => new Promise((resolve, reject) => {
    debug('rm', { path })
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
