import testStore from 'abstract-tus-store'
import aws from 'aws-sdk'
// import test from 'blue-tape'
import initS3Store from '../src'

const {
  accessKeyId = 'AKIAIOSFODNN7EXAMPLE',
  secretAccessKey = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
  // docker-machine ip default
  endpoint = 'http://127.0.0.1:9000',
  bucket = 's3-tus-store',
} = {
  accessKeyId: process.env.S3_ACCESS_KEY,
  secretAccessKey: process.env.S3_SECRET_KEY,
  endpoint: process.env.S3_ENDPOINT,
  bucket: process.env.S3_BUCKET,
}

const client = new aws.S3({
  accessKeyId,
  secretAccessKey,
  endpoint: new aws.Endpoint(endpoint),
  s3ForcePathStyle: true, // needed for minio
  signatureVersion: 'v4',
  s3DisableBodySigning: true,
})

// hmmmm, tmp workaround for https://github.com/aws/aws-sdk-js/issues/965#issuecomment-247930423
client.shouldDisableBodySigning = () => true

const Bucket = bucket

const clearBucket = async () => {
  const { Contents } = await client.listObjects({ Bucket }).promise()
  const tasks = Contents.map(({ Key }) => (
    client.deleteObject({ Key, Bucket }).promise()
  ))
  return Promise.all(tasks)
}

const createBucket = async () => {
  try {
    await clearBucket()
    await client.deleteBucket({ Bucket }).promise()
  } catch (err) {
    // ignore NoSuchBucket errors
    if (err.code !== 'NoSuchBucket') {
      throw err
    }
  }
  await client.createBucket({ Bucket }).promise()
}

const setup = async () => {
  await createBucket()
  return initS3Store({ client, bucket })
}

const teardown = async () => {
  await clearBucket()
  return client.deleteBucket({ Bucket }).promise()
}

testStore({ setup, teardown })
