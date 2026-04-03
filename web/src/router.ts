import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  { path: '/', redirect: '/files' },
  { path: '/files', component: () => import('./views/FileList.vue'), meta: { title: 'Files' } },
  { path: '/tasks', component: () => import('./views/Tasks.vue'), meta: { title: 'Tasks' } },
  { path: '/upload', component: () => import('./views/Upload.vue'), meta: { title: 'Upload' } },
  { path: '/system', component: () => import('./views/System.vue'), meta: { title: 'System' } },
]

export const router = createRouter({
  history: createWebHistory(),
  routes,
})
