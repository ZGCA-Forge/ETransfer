import { ref, onMounted } from 'vue'
import { listSources, listSinks, type PluginInfo } from '../api/client'

export function usePlugins() {
  const sources = ref<PluginInfo[]>([])
  const sinks = ref<PluginInfo[]>([])
  const loading = ref(false)

  async function refresh() {
    loading.value = true
    try {
      const [s, k] = await Promise.all([listSources(), listSinks()])
      sources.value = s.data
      sinks.value = k.data
    } finally {
      loading.value = false
    }
  }

  onMounted(refresh)

  return { sources, sinks, loading, refresh }
}
