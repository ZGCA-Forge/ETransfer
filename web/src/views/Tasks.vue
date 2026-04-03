<template>
  <n-space vertical :size="16">
    <n-page-header title="Transfer Tasks" subtitle="Download + push pipeline" />

    <n-card title="Create Task">
      <n-form label-placement="left" label-width="120">
        <n-form-item label="Source URL">
          <n-input v-model:value="form.source_url" placeholder="https://example.com/file.zip" @blur="onUrlBlur" />
        </n-form-item>
        <n-form-item label="Source Plugin">
          <n-tag v-if="resolvedSource">{{ resolvedSource }}</n-tag>
          <span v-else style="color: #999">Paste URL to auto-detect</span>
        </n-form-item>
        <n-form-item label="Retention">
          <n-select v-model:value="form.retention" :options="retentionOpts" style="width: 200px" />
        </n-form-item>
        <n-form-item v-if="form.retention === 'ttl'" label="TTL (seconds)">
          <n-input-number v-model:value="form.retention_ttl" :min="60" />
        </n-form-item>
        <n-form-item label="Push to Sink">
          <n-select
            v-model:value="form.sink_plugin"
            :options="sinkOptions"
            clearable
            placeholder="None (local only)"
            style="width: 260px"
          />
        </n-form-item>
        <n-form-item v-if="form.sink_plugin && sinkSchema" v-for="(def, key) in sinkSchema" :key="key" :label="String(key)">
          <n-input
            v-if="def.secret"
            v-model:value="sinkConfig[String(key)]"
            type="password"
            show-password-on="click"
            :placeholder="def.description || String(key)"
          />
          <n-input v-else v-model:value="sinkConfig[String(key)]" :placeholder="def.description || String(key)" />
        </n-form-item>
        <n-form-item>
          <n-button type="primary" @click="submitTask" :loading="submitting">Create Task</n-button>
        </n-form-item>
      </n-form>
    </n-card>

    <n-space>
      <n-button @click="refreshTasks" :loading="loadingTasks">Refresh</n-button>
    </n-space>
    <n-data-table :columns="taskColumns" :data="tasks" :loading="loadingTasks" />
  </n-space>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, h, reactive, watch } from 'vue'
import { NButton, NTag, NProgress, useMessage } from 'naive-ui'
import { listTasks, createTask, cancelTask, resolveSource, type TaskInfo, type PluginInfo } from '../api/client'
import { usePlugins } from '../composables/usePlugins'

const msg = useMessage()
const { sinks } = usePlugins()

const form = reactive({
  source_url: '',
  sink_plugin: null as string | null,
  retention: 'permanent',
  retention_ttl: 3600 as number | null,
})

const sinkConfig = reactive<Record<string, string>>({})
const resolvedSource = ref('')
const submitting = ref(false)

const retentionOpts = [
  { label: 'Permanent', value: 'permanent' },
  { label: 'Download Once', value: 'download_once' },
  { label: 'TTL', value: 'ttl' },
]

const sinkOptions = computed(() => sinks.value.map((s: PluginInfo) => ({ label: s.display_name, value: s.name })))

const sinkSchema = computed(() => {
  if (!form.sink_plugin) return null
  const s = sinks.value.find((s: PluginInfo) => s.name === form.sink_plugin)
  return s?.config_schema || null
})

async function onUrlBlur() {
  if (!form.source_url) { resolvedSource.value = ''; return }
  try {
    const { data } = await resolveSource(form.source_url)
    resolvedSource.value = data.display_name
  } catch { resolvedSource.value = '' }
}

async function submitTask() {
  if (!form.source_url) { msg.warning('Please enter a URL'); return }
  submitting.value = true
  try {
    await createTask({
      source_url: form.source_url,
      sink_plugin: form.sink_plugin || undefined,
      sink_config: form.sink_plugin ? { ...sinkConfig } : undefined,
      retention: form.retention,
      retention_ttl: form.retention === 'ttl' ? form.retention_ttl ?? undefined : undefined,
    })
    msg.success('Task created')
    form.source_url = ''
    resolvedSource.value = ''
    refreshTasks()
  } catch (e: any) {
    msg.error(e?.response?.data?.detail || 'Failed')
  } finally {
    submitting.value = false
  }
}

const tasks = ref<TaskInfo[]>([])
const loadingTasks = ref(false)

async function refreshTasks() {
  loadingTasks.value = true
  try {
    const { data } = await listTasks()
    tasks.value = data
  } finally {
    loadingTasks.value = false
  }
}

const taskColumns = [
  { title: 'Filename', key: 'filename', ellipsis: { tooltip: true }, width: 200 },
  { title: 'Source', key: 'source_plugin', width: 100 },
  { title: 'Sink', key: 'sink_plugin', width: 80, render: (r: TaskInfo) => r.sink_plugin || '-' },
  {
    title: 'Status',
    key: 'status',
    width: 110,
    render: (r: TaskInfo) => {
      const typeMap: Record<string, string> = {
        completed: 'success', failed: 'error', cancelled: 'warning',
        downloading: 'info', pushing: 'info', pending: 'default',
      }
      return h(NTag, { type: (typeMap[r.status] || 'default') as any, size: 'small' }, () => r.status)
    },
  },
  {
    title: 'Progress',
    key: 'progress',
    width: 120,
    render: (r: TaskInfo) => h(NProgress, { percentage: Math.round(r.progress * 100), type: 'line' }),
  },
  { title: 'Retention', key: 'retention', width: 110 },
  {
    title: 'Actions',
    key: 'actions',
    width: 100,
    render: (r: TaskInfo) =>
      r.status === 'downloading' || r.status === 'pushing' || r.status === 'pending'
        ? h(NButton, { size: 'small', type: 'error', onClick: () => cancelTask(r.task_id).then(refreshTasks) }, () => 'Cancel')
        : '',
  },
]

onMounted(refreshTasks)
</script>
