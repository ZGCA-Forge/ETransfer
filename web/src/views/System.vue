<template>
  <n-space vertical :size="16">
    <n-page-header title="System Info" subtitle="Server status" />
    <n-spin :show="loading">
      <n-descriptions bordered :column="2" v-if="info">
        <n-descriptions-item label="Version">{{ info.version }}</n-descriptions-item>
        <n-descriptions-item label="TUS Version">{{ info.tus_version }}</n-descriptions-item>
        <n-descriptions-item label="Max Upload Size">{{ info.max_upload_size || 'Unlimited' }}</n-descriptions-item>
        <n-descriptions-item label="Extensions">{{ info.extensions?.join(', ') }}</n-descriptions-item>
      </n-descriptions>
    </n-spin>

    <n-card title="Plugins">
      <n-tabs>
        <n-tab-pane name="sources" tab="Sources">
          <n-data-table :columns="sourceColumns" :data="sources" size="small" />
        </n-tab-pane>
        <n-tab-pane name="sinks" tab="Sinks">
          <n-data-table :columns="sinkColumns" :data="sinksList" size="small" />
        </n-tab-pane>
      </n-tabs>
    </n-card>
  </n-space>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { getServerInfo } from '../api/client'
import { usePlugins } from '../composables/usePlugins'

const info = ref<any>(null)
const loading = ref(true)
const { sources, sinks: sinksList } = usePlugins()

const sourceColumns = [
  { title: 'Name', key: 'name' },
  { title: 'Display', key: 'display_name' },
  { title: 'Hosts', key: 'supported_hosts', render: (r: any) => (r.supported_hosts || []).join(', ') || '*' },
  { title: 'Priority', key: 'priority' },
]

const sinkColumns = [
  { title: 'Name', key: 'name' },
  { title: 'Display', key: 'display_name' },
  { title: 'Multipart', key: 'supports_multipart', render: (r: any) => r.supports_multipart ? 'Yes' : 'No' },
]

onMounted(async () => {
  try {
    const { data } = await getServerInfo()
    info.value = data
  } finally {
    loading.value = false
  }
})
</script>
