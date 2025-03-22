import { config, getProvider } from '@genoacms/cloudabstraction'
import * as Minio from 'minio'
import { join } from 'node:path'
import { resolve } from 'node:path/posix'

/**
 * @import {Adapter} from '@genoacms/cloudabstraction/storage'
 */
const ADAPTER_PATH = '@genoacms/adapter-minio'

const provider = getProvider('storage', ADAPTER_PATH)
const minioClient = new Minio.Client(provider.config)
console.log(provider)

const isBucketRegistered = (name) => {
  const has = config.storage.buckets.find((bucket) => bucket.name === name &&
    bucket.providerName === provider.name)
  return !!has
}
const checkBucket = async (name) => {
  const isRegistered = isBucketRegistered(name)
  if (!isRegistered) {
    throw new Error('bucket-unregistered')
  }
  const exists = await minioClient.bucketExists(name)
  if (!exists) {
    throw new Error('bucket-dont-exists')
  }
}

/**
 * @type {Adapter['getObject']}
 */
const getObject = async ({ bucket, name }) => {
  checkBucket(bucket)
  const data = await minioClient.getObject(bucket, name)
  return {
    data
  }
}

/**
 * @type {Adapter['getSignedURL']}
 */
const getSignedURL = async ({ bucket, name }, expires) => {
  checkBucket(bucket)
  const url = await minioClient.presignedGetObject(bucket, name, expires)
  return url
}

/**
 * @type {Adapter['uploadObject']}
 */
const uploadObject = async ({ bucket, name }, stream, options = {}) => {
  checkBucket(bucket)
  await minioClient.putObject(bucket, name, stream)
}

/**
 * @type {Adapter['moveObject']}
 */
const moveObject = async ({ bucket, name }, newName) => {
  checkBucket(bucket)
  await minioClient.copyObject(bucket, newName, `/${bucket}/${name}`)
  await minioClient.removeObject(bucket, name)
}

/**
 * @type {Adapter['deleteObject']}
 */
const deleteObject = async ({ bucket, name }) => {
  checkBucket(bucket)
  await minioClient.removeObject(bucket, name)
}
/**
 * @type {Adapter['listDirectory']}
 */
const listDirectory = async ({ bucket, name }, listingParams = {}) => {
  checkBucket(bucket)
  const prefix = join(name, '/')
  const startAfter = listingParams.startAfter
  const stream = minioClient.listObjectsV2(bucket, prefix, false, startAfter)
  const contents = await parseDirectory(bucket, stream)
  return contents
}

function parseDirectory (bucket, stream) {
  return new Promise((resolve, reject) => {
    const contents = {
      files: [],
      directories: []
    }
    stream.on('data', function (obj) {
      if (obj.hasOwnProperty('name')) {
        const file = {
          bucket,
          name: obj.name
        }
        contents.files.push(file)
      } else {
        const prefix = {
          bucket,
          name: obj.prefix
        }
        contents.directories.push(prefix)
      }
    })
    stream.on('error', function (err) {
      console.log(err)
      reject(new Error('Failed to list directory'))
    })
    stream.on('close', function () {
      resolve(contents)
    })
  })
}

// await listDirectory({ bucket: 'genoacms', name: 'tettt/' })
// getObject({ bucket: 'genoacms', name: 'tettt/20250308_112255.jpg' })

export {
  getObject,
  getSignedURL,
  uploadObject,
  moveObject,
  deleteObject,
  listDirectory
}
