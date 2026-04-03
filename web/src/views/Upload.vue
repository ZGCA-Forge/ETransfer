<template>
  <n-space vertical :size="16">
    <n-page-header title="Upload" subtitle="TUS resumable upload" />
    <n-card>
      <n-form label-placement="left" label-width="120">
        <n-form-item label="Select Files">
          <n-upload
            :custom-request="handleUpload"
            :multiple="true"
            directory-dnd
            :show-file-list="true"
          >
            <n-button>Choose Files</n-button>
          </n-upload>
        </n-form-item>
        <n-form-item label="Folder Upload">
          <input type="file" ref="folderInput" webkitdirectory multiple @change="onFolderSelect" />
        </n-form-item>
        <n-form-item label="Retention">
          <n-select v-model:value="retention" :options="retentionOpts" style="width: 200px" />
        </n-form-item>
        <n-form-item v-if="retention === 'ttl'" label="TTL (seconds)">
          <n-input-number v-model:value="retentionTtl" :min="60" />
        </n-form-item>
      </n-form>
    </n-card>
    <n-card v-if="uploads.length" title="Upload Progress">
      <n-space vertical>
        <div v-for="u in uploads" :key="u.name" style="margin-bottom: 8px">
          <n-text>{{ u.name }}</n-text>
          <n-progress :percentage="u.progress" :status="u.error ? 'error' : u.progress === 100 ? 'success' : 'default'" />
        </div>
      </n-space>
    </n-card>
  </n-space>
</template>

<script setup lang="ts">
import { ref, reactive } from 'vue'
import { useMessage } from 'naive-ui'
import * as tus from 'tus-js-client'

const msg = useMessage()
const retention = ref('permanent')
const retentionTtl = ref(3600)
const folderInput = ref<HTMLInputElement>()

interface UploadEntry { name: string; progress: number; error?: string }
const uploads = reactive<UploadEntry[]>([])

const retentionOpts = [
  { label: 'Permanent', value: 'permanent' },
  { label: 'Download Once', value: 'download_once' },
  { label: 'TTL', value: 'ttl' },
]

function tusUpload(file: File, relativePath?: string) {
  const entry: UploadEntry = { name: relativePath || file.name, progress: 0 }
  uploads.push(entry)

  const metadata: Record<string, string> = {
    filename: file.name,
    filetype: file.type || 'application/octet-stream',
    retention: retention.value,
  }
  if (retention.value === 'ttl') {
    metadata.retention_ttl = String(retentionTtl.value)
  }
  if (relativePath) {
    metadata.relativePath = relativePath
  }

  const upload = new tus.Upload(file, {
    endpoint: '/tus',
    retryDelays: [0, 1000, 3000, 5000],
    chunkSize: 32 * 1024 * 1024,
    metadata,
    onProgress(bytesUploaded, bytesTotal) {
      entry.progress = Math.round((bytesUploaded / bytesTotal) * 100)
    },
    onSuccess() {
      entry.progress = 100
    },
    onError(error) {
      entry.error = error.message
      msg.error(`Upload failed: ${file.name}`)
    },
  })
  upload.start()
}

function handleUpload({ file }: { file: { file: File } }) {
  tusUpload(file.file)
}

function onFolderSelect(e: Event) {
  const input = e.target as HTMLInputElement
  if (!input.files) return
  for (const file of Array.from(input.files)) {
    tusUpload(file, (file as any).webkitRelativePath || file.name)
  }
}
</script>
