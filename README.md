# s3-tus-store

[![Build Status](https://travis-ci.org/blockai/s3-tus-store.svg?branch=master)](https://travis-ci.org/blockai/s3-tus-store)

[![tus-store-compatible](https://github.com/blockai/abstract-tus-store/raw/master/badge.png)](https://github.com/blockai/abstract-tus-store)

This store is also compatible with [minio](https://github.com/minio/minio) (see [test/](./test/index.test.js)).

## Install

```bash
npm install --save s3-tus-store
```

Requires Node v6+

## Usage

```javascript
import s3TusStore from 's3-tus-store'
import aws from 'aws-sdk'

const store = s3TusStore({
  bucket: 'my-awesome-s3-bucket',
  client: new aws.S3({
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
  }),
})
```

See
[abstract-tus-store](https://github.com/blockai/abstract-tus-store#api)
for API documentation.