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
  region,
} = {
  accessKeyId: process.env.S3_ACCESS_KEY,
  secretAccessKey: process.env.S3_SECRET_KEY,
  endpoint: process.env.S3_ENDPOINT,
  bucket: process.env.S3_BUCKET,
  region: process.env.S3_REGION,
}

const s3Config = {
  accessKeyId,
  secretAccessKey,
  region,
  endpoint: new aws.Endpoint(endpoint),
  s3ForcePathStyle: true, // needed for minio
  signatureVersion: 'v4',
}

const client = new aws.S3(s3Config)

// hmmmm, tmp workaround for https://github.com/aws/aws-sdk-js/issues/965#issuecomment-247930423
if (endpoint.startsWith('http://')) {
  client.shouldDisableBodySigning = () => true
}

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
    // await client.deleteBucket({ Bucket }).promise()
  } catch (err) {
    // ignore NoSuchBucket errors
    if (err.code !== 'NoSuchBucket') {
      throw err
    }
  }
  try {
    await client.createBucket({ Bucket }).promise()
  } catch (err) {
    // ignore "bucket already exists" errors
    if (err.code !== 'BucketAlreadyOwnedByYou') {
      throw err
    }
  }
}

const setup = async () => {
  await createBucket()
  return initS3Store({ client, bucket })
}

const teardown = async () => {
  await clearBucket()
}

testStore({ setup, teardown })
