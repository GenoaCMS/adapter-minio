import { config, getProvider } from '@genoacms/cloudabstraction'
import * as Minio from 'minio'

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

export {
  getObject,
  getSignedURL,
  uploadObject,
  moveObject,
  deleteObject
}
