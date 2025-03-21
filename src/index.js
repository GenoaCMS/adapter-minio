import { getProvider } from '@genoacms/cloudabstraction'
import * as Minio from 'minio'

const ADAPTER_PATH = '@genoacms/adapter-minio'

const provider = getProvider('storage', ADAPTER_PATH)
const minioClient = new Minio.Client(provider.config)
console.log(provider)
