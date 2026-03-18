import { reactive } from 'vue'

const state = reactive({
  checked: false,
  configured: false
})

export function setSetupStatus(configured) {
  state.checked = true
  state.configured = configured
}

export function getSetupState() {
  return state
}

export default state
