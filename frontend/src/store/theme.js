import { ref, watch } from 'vue'

const STORAGE_KEY = 'mirofish-theme'
const isDark = ref(localStorage.getItem(STORAGE_KEY) === 'dark')

function applyTheme(dark) {
  document.documentElement.classList.toggle('dark', dark)
  localStorage.setItem(STORAGE_KEY, dark ? 'dark' : 'light')
}

// Apply on init
applyTheme(isDark.value)

watch(isDark, applyTheme)

export function useTheme() {
  return {
    isDark,
    toggle: () => { isDark.value = !isDark.value }
  }
}
