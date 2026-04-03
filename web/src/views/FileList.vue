<template>
  <n-space vertical :size="16">
    <n-page-header title="Files" subtitle="Server file list" />
    <n-space>
      <n-input v-model:value="search" placeholder="Search filename..." clearable style="width: 260px" />
      <n-button @click="refresh" :loading="loading">Refresh</n-button>
    </n-space>
    <n-data-table :columns="columns" :data="filtered" :loading="loading" :pagination="pagination" />
  </n-space>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, h } from 'vue'
import { NButton, NTag, NSpace, useMessage } from 'naive-ui'
import { listFiles, deleteFile, downloadFileUrl, type FileInfo } from '../api/client'

const msg = useMessage()
const files = ref<FileInfo[]>([])
const total = ref(0)
const loading = ref(false)
const search = ref('')
const page = ref(1)
const pageSize = 20

async function refresh() {
  loading.value = true
  try {
    const { data } = await listFiles(page.value, pageSize)
    files.value = data.files
    total.value = data.total
  } finally {
    loading.value = false
  }
}

const filtered = computed(() => {
  if (!search.value) return files.value
  const q = search.value.toLowerCase()
  return files.value.filter(f => f.filename.toLowerCase().includes(q))
})

const pagination = { pageSize }

function fmtSize(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`
}

const columns = [
  { title: 'Filename', key: 'filename', ellipsis: { tooltip: true } },
  {
    title: 'Size',
    key: 'size',
    width: 110,
    render: (row: FileInfo) => fmtSize(row.size),
  },
  {
    title: 'Progress',
    key: 'progress',
    width: 100,
    render: (row: FileInfo) => {
      const pct = row.size > 0 ? Math.round((row.uploaded_size / row.size) * 100) : 0
      return `${pct}%`
    },
  },
  {
    title: 'Status',
    key: 'status',
    width: 100,
    render: (row: FileInfo) =>
      h(NTag, { type: row.status === 'complete' ? 'success' : 'warning', size: 'small' }, () => row.status),
  },
  {
    title: 'Retention',
    key: 'retention',
    width: 120,
    render: (row: FileInfo) => row.metadata?.retention || 'permanent',
  },
  {
    title: 'Actions',
    key: 'actions',
    width: 180,
    render: (row: FileInfo) =>
      h(NSpace, { size: 'small' }, () => [
        h(
          NButton,
          {
            size: 'small',
            onClick: () => window.open(downloadFileUrl(row.file_id), '_blank'),
            disabled: row.status !== 'complete',
          },
          () => 'Download',
        ),
        h(
          NButton,
          {
            size: 'small',
            type: 'error',
            onClick: async () => {
              await deleteFile(row.file_id)
              msg.success('Deleted')
              refresh()
            },
          },
          () => 'Delete',
        ),
      ]),
  },
]

onMounted(refresh)
</script>
