import axios from 'axios'

const api = axios.create({ baseURL: '/' })

export interface FileInfo {
  file_id: string
  filename: string
  size: number
  uploaded_size: number
  status: string
  mime_type?: string
  created_at?: string
  updated_at?: string
  metadata?: Record<string, any>
}

export interface FileListResponse {
  files: FileInfo[]
  total: number
  page: number
  page_size: number
}

export interface TaskInfo {
  task_id: string
  source_url: string
  source_plugin: string
  sink_plugin?: string
  status: string
  progress: number
  downloaded_bytes: number
  pushed_parts: number
  error?: string
  filename?: string
  file_size?: number
  file_id?: string
  retention: string
  retention_ttl?: number
  sink_result_url?: string
  created_at: string
  updated_at: string
  completed_at?: string
}

export interface PluginInfo {
  name: string
  display_name: string
  type: string
  supported_hosts?: string[]
  config_schema?: Record<string, any>
}

export const listFiles = (page = 1, pageSize = 20) =>
  api.get<FileListResponse>('/api/files', { params: { page, page_size: pageSize } })

export const deleteFile = (fileId: string) =>
  api.delete(`/api/files/${fileId}`)

export const downloadFileUrl = (fileId: string) =>
  `/api/files/${fileId}/download`

export const listTasks = () =>
  api.get<TaskInfo[]>('/api/tasks')

export const createTask = (data: {
  source_url: string
  sink_plugin?: string
  sink_config?: Record<string, any>
  retention?: string
  retention_ttl?: number
}) => api.post<TaskInfo>('/api/tasks', data)

export const cancelTask = (taskId: string) =>
  api.delete(`/api/tasks/${taskId}`)

export const listSources = () =>
  api.get<PluginInfo[]>('/api/plugins/sources')

export const listSinks = () =>
  api.get<PluginInfo[]>('/api/plugins/sinks')

export const resolveSource = (url: string) =>
  api.get<{ source_plugin: string; display_name: string }>('/api/plugins/resolve', { params: { url } })

export const getServerInfo = () =>
  api.get('/api/info')

export default api
