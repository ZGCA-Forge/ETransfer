<template>
  <n-config-provider :theme="darkTheme" :locale="zhCN" :date-locale="dateZhCN">
    <n-layout has-sider style="height: 100vh">
      <n-layout-sider
        bordered
        :width="200"
        :collapsed-width="64"
        collapse-mode="width"
        show-trigger
      >
        <div style="padding: 20px 16px 12px; font-size: 18px; font-weight: 700; text-align: center">
          EasyTransfer
        </div>
        <n-menu
          :value="activeKey"
          :options="menuOptions"
          @update:value="handleMenu"
        />
      </n-layout-sider>
      <n-layout-content content-style="padding: 24px">
        <router-view />
      </n-layout-content>
    </n-layout>
  </n-config-provider>
</template>

<script setup lang="ts">
import { computed, h } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { darkTheme, zhCN, dateZhCN, type MenuOption } from 'naive-ui'
import {
  FolderOpenOutline,
  CloudDownloadOutline,
  CloudUploadOutline,
  InformationCircleOutline,
} from '@vicons/ionicons5'
import { NIcon } from 'naive-ui'

const router = useRouter()
const route = useRoute()

const activeKey = computed(() => route.path)

function icon(comp: any) {
  return () => h(NIcon, null, { default: () => h(comp) })
}

const menuOptions: MenuOption[] = [
  { label: 'Files', key: '/files', icon: icon(FolderOpenOutline) },
  { label: 'Tasks', key: '/tasks', icon: icon(CloudDownloadOutline) },
  { label: 'Upload', key: '/upload', icon: icon(CloudUploadOutline) },
  { label: 'System', key: '/system', icon: icon(InformationCircleOutline) },
]

function handleMenu(key: string) {
  router.push(key)
}
</script>

<style>
body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }
</style>
