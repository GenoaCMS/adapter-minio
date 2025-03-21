import credentials from './credentials.json' with { type: 'json' }

/**
 * @type {import('@genoacms/cloudabstraction').Config}
 */
const config = {
  storage: {
    defaultBucket: 'genoacms',
    buckets: [
      {
        name: 'genoacms',
        providerName: 'minio'
      },
      {
        name: 'genoacms-public',
        providerName: 'minio'
      }
    ],
    providers: [
      {
        name: 'minio',
        adapterPath: '@genoacms/adapter-minio',
        adapter: import('../src/index.js'),
        config: {
          endPoint: 'localhost',
          port: 9000,
          accessKey: credentials.accessKey,
          secretKey: credentials.secretKey
        }
      }
    ]
  }
}

export default config
