<template>
  <div class="setup-container">
    <nav class="navbar">
      <div class="nav-brand">MIROFISH</div>
      <div class="nav-links">
        <span class="setup-label">Configuration</span>
      </div>
    </nav>

    <div class="setup-content">
      <div class="setup-card">
        <div class="card-header">
          <h1>Setup</h1>
          <p class="subtitle">Configure your API keys to get started with MiroFish.</p>
        </div>

        <!-- LLM Configuration -->
        <div class="config-section">
          <div class="section-title">
            <span class="section-num">01</span>
            LLM Configuration
          </div>
          <p class="section-desc">OpenAI-compatible API. Use Ollama for fully local, free inference — or any compatible provider.</p>

          <!-- Preset buttons -->
          <div class="preset-row">
            <button class="preset-btn" @click="applyOllamaPreset">Ollama (Local)</button>
            <button class="preset-btn" @click="applyOpenAIPreset">OpenAI</button>
          </div>

          <div class="form-group">
            <label>API Key (enter "ollama" for local Ollama)</label>
            <div class="input-with-toggle">
              <input
                :type="showLlmKey ? 'text' : 'password'"
                v-model="form.llm_api_key"
                placeholder="ollama  /  sk-..."
                class="form-input"
              />
              <button class="toggle-btn" @click="showLlmKey = !showLlmKey">
                {{ showLlmKey ? 'Hide' : 'Show' }}
              </button>
            </div>
          </div>

          <div class="form-row">
            <div class="form-group">
              <label>Base URL</label>
              <input
                type="text"
                v-model="form.llm_base_url"
                placeholder="http://localhost:11434/v1"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label>Model Name</label>
              <input
                type="text"
                v-model="form.llm_model_name"
                placeholder="qwen2.5:7b"
                class="form-input"
              />
            </div>
          </div>

          <div class="form-group">
            <label>Embedding Model</label>
            <input
              type="text"
              v-model="form.embed_model"
              placeholder="nomic-embed-text / text-embedding-3-small"
              class="form-input"
            />
          </div>
        </div>

        <!-- Optional Boost Config -->
        <div class="config-section">
          <button class="collapse-toggle" @click="showBoost = !showBoost">
            <span class="section-num">02</span>
            Acceleration LLM (Optional)
            <span class="toggle-arrow">{{ showBoost ? '−' : '+' }}</span>
          </button>

          <div v-if="showBoost" class="boost-fields">
            <p class="section-desc">Optional secondary LLM for parallel simulation acceleration.</p>
            <div class="form-group">
              <label>Boost API Key</label>
              <input type="password" v-model="form.llm_boost_api_key" class="form-input" placeholder="Optional" />
            </div>
            <div class="form-row">
              <div class="form-group">
                <label>Boost Base URL</label>
                <input type="text" v-model="form.llm_boost_base_url" class="form-input" placeholder="Optional" />
              </div>
              <div class="form-group">
                <label>Boost Model Name</label>
                <input type="text" v-model="form.llm_boost_model_name" class="form-input" placeholder="Optional" />
              </div>
            </div>
          </div>
        </div>

        <!-- Status Messages -->
        <div v-if="statusMessage" :class="['status-message', statusType]">
          {{ statusMessage }}
        </div>

        <!-- Actions -->
        <div class="action-row">
          <button
            class="validate-btn"
            @click="validateKeys"
            :disabled="!canValidate || validating"
          >
            {{ validating ? 'Validating...' : 'Validate Keys' }}
          </button>
          <button
            class="save-btn"
            @click="saveAndContinue"
            :disabled="!canSave || saving"
          >
            {{ saving ? 'Saving...' : 'Save & Continue' }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRouter } from 'vue-router'
import { getSetupStatus, saveSetup, validateSetup } from '../api/setup'
import { setSetupStatus } from '../store/setupStatus'

const router = useRouter()

const form = ref({
  llm_api_key: 'ollama',
  llm_base_url: 'http://localhost:11434/v1',
  llm_model_name: 'qwen2.5:7b',
  embed_model: 'nomic-embed-text',
  llm_boost_api_key: '',
  llm_boost_base_url: '',
  llm_boost_model_name: '',
})

const showLlmKey = ref(false)
const showBoost = ref(false)
const validating = ref(false)
const saving = ref(false)
const statusMessage = ref('')
const statusType = ref('info')

const canValidate = computed(() => {
  return form.value.llm_base_url.trim() !== '' && form.value.llm_model_name.trim() !== ''
})

const canSave = computed(() => {
  return form.value.llm_base_url.trim() !== '' && form.value.llm_model_name.trim() !== ''
})

const applyOllamaPreset = () => {
  form.value.llm_api_key = 'ollama'
  form.value.llm_base_url = 'http://localhost:11434/v1'
  form.value.llm_model_name = 'qwen2.5:7b'
  form.value.embed_model = 'nomic-embed-text'
}

const applyOpenAIPreset = () => {
  form.value.llm_api_key = ''
  form.value.llm_base_url = 'https://api.openai.com/v1'
  form.value.llm_model_name = 'gpt-4o-mini'
  form.value.embed_model = 'text-embedding-3-small'
}

// Load existing config on mount
const loadExisting = async () => {
  try {
    const res = await getSetupStatus()
    if (res.llm_base_url) form.value.llm_base_url = res.llm_base_url
    if (res.llm_model_name) form.value.llm_model_name = res.llm_model_name
    if (res.embed_model) form.value.embed_model = res.embed_model
    if (res.llm_api_key) form.value.llm_api_key = res.llm_api_key
  } catch (e) {
    // Backend may not be ready yet
  }
}
loadExisting()

const validateKeys = async () => {
  validating.value = true
  statusMessage.value = ''
  try {
    const res = await validateSetup(form.value)
    if (res.valid) {
      statusMessage.value = 'All keys validated successfully!'
      statusType.value = 'success'
    } else {
      const errors = res.errors || []
      statusMessage.value = 'Validation failed: ' + errors.join('; ')
      statusType.value = 'error'
    }
  } catch (e) {
    statusMessage.value = 'Validation request failed. Is the backend running?'
    statusType.value = 'error'
  } finally {
    validating.value = false
  }
}

const saveAndContinue = async () => {
  saving.value = true
  statusMessage.value = ''
  try {
    const res = await saveSetup(form.value)
    if (res.configured) {
      setSetupStatus(true)
      statusMessage.value = 'Configuration saved!'
      statusType.value = 'success'
      setTimeout(() => {
        router.push({ name: 'Home' })
      }, 500)
    } else {
      statusMessage.value = 'LLM Base URL and Model Name are required.'
      statusType.value = 'error'
    }
  } catch (e) {
    statusMessage.value = 'Failed to save configuration. Is the backend running?'
    statusType.value = 'error'
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
.setup-container {
  min-height: 100vh;
  background: var(--c-bg);
  font-family: 'Space Grotesk', 'Noto Sans SC', system-ui, sans-serif;
  color: var(--c-text);
}

.navbar {
  height: 60px;
  background: var(--c-inverse-bg);
  color: var(--c-inverse-text);
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 40px;
}

.nav-brand {
  font-family: 'JetBrains Mono', monospace;
  font-weight: 800;
  letter-spacing: 1px;
  font-size: 1.2rem;
}

.setup-label {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.9rem;
  color: #999;
}

.setup-content {
  max-width: 700px;
  margin: 0 auto;
  padding: 60px 20px;
}

.setup-card {
  border: 1px solid var(--c-border-light);
  padding: 40px;
  background: var(--c-surface);
}

.card-header {
  margin-bottom: 40px;
}

.card-header h1 {
  font-size: 2rem;
  font-weight: 520;
  margin: 0 0 10px 0;
  color: var(--c-text);
}

.subtitle {
  color: var(--c-text-muted);
  font-size: 0.95rem;
}

.config-section {
  margin-bottom: 30px;
  padding-bottom: 30px;
  border-bottom: 1px solid var(--c-surface-3);
}

.config-section:last-of-type {
  border-bottom: none;
}

.section-title {
  font-weight: 520;
  font-size: 1.1rem;
  margin-bottom: 8px;
  display: flex;
  align-items: center;
  gap: 12px;
  color: var(--c-text);
}

.section-num {
  font-family: 'JetBrains Mono', monospace;
  color: var(--c-accent);
  font-weight: 700;
  font-size: 0.8rem;
}

.section-desc {
  color: var(--c-text-muted);
  font-size: 0.85rem;
  margin-bottom: 20px;
}

.section-desc a {
  color: var(--c-accent);
  text-decoration: none;
}

.preset-row {
  display: flex;
  gap: 8px;
  margin-bottom: 16px;
}

.preset-btn {
  padding: 8px 16px;
  border: 1px solid var(--c-border);
  background: var(--c-surface);
  color: var(--c-text);
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.8rem;
  font-weight: 700;
  cursor: pointer;
  letter-spacing: 0.3px;
  transition: all 0.15s;
}

.preset-btn:hover {
  background: var(--c-inverse-bg);
  color: var(--c-inverse-text);
  border-color: var(--c-inverse-bg);
}

.form-group {
  margin-bottom: 16px;
}

.form-group label {
  display: block;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  color: var(--c-text-muted);
  margin-bottom: 6px;
  letter-spacing: 0.5px;
}

.form-input {
  width: 100%;
  padding: 12px 16px;
  border: 1px solid var(--c-border-light);
  background: var(--c-surface-2);
  color: var(--c-text);
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.9rem;
  outline: none;
  box-sizing: border-box;
}

.form-input:focus {
  border-color: var(--c-border);
  background: var(--c-surface);
}

.input-with-toggle {
  display: flex;
  gap: 8px;
}

.input-with-toggle .form-input {
  flex: 1;
}

.toggle-btn {
  padding: 0 16px;
  border: 1px solid var(--c-border-light);
  background: var(--c-surface);
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  cursor: pointer;
  color: var(--c-text-muted);
}

.toggle-btn:hover {
  background: var(--c-surface-3);
}

.form-row {
  display: flex;
  gap: 16px;
}

.form-row .form-group {
  flex: 1;
}

.collapse-toggle {
  background: none;
  border: none;
  font-weight: 520;
  font-size: 1.1rem;
  display: flex;
  align-items: center;
  gap: 12px;
  cursor: pointer;
  width: 100%;
  text-align: left;
  padding: 0;
  color: var(--c-text);
}

.toggle-arrow {
  margin-left: auto;
  font-size: 1.2rem;
  color: var(--c-text-muted);
}

.boost-fields {
  margin-top: 16px;
}

.status-message {
  padding: 12px 16px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.85rem;
  margin-bottom: 20px;
}

.status-message.success {
  background: #1a2e1a;
  color: #66bb6a;
  border: 1px solid #2e5a2e;
}

html:not(.dark) .status-message.success {
  background: #E8F5E9;
  color: #2E7D32;
  border: 1px solid #C8E6C9;
}

.status-message.error {
  background: #2e1a1a;
  color: #ef9a9a;
  border: 1px solid #5a2e2e;
}

html:not(.dark) .status-message.error {
  background: #FFEBEE;
  color: #C62828;
  border: 1px solid #FFCDD2;
}

.status-message.info {
  background: var(--c-surface-2);
  color: var(--c-text-muted);
  border: 1px solid var(--c-border-light);
}

.action-row {
  display: flex;
  gap: 12px;
}

.validate-btn, .save-btn {
  flex: 1;
  padding: 16px;
  font-family: 'JetBrains Mono', monospace;
  font-weight: 700;
  font-size: 0.95rem;
  cursor: pointer;
  letter-spacing: 0.5px;
  transition: all 0.2s;
}

.validate-btn {
  background: var(--c-surface);
  color: var(--c-text);
  border: 1px solid var(--c-border);
}

.validate-btn:hover:not(:disabled) {
  background: var(--c-surface-3);
}

.save-btn {
  background: var(--c-inverse-bg);
  color: var(--c-inverse-text);
  border: 1px solid var(--c-inverse-bg);
}

.save-btn:hover:not(:disabled) {
  background: var(--c-accent);
  border-color: var(--c-accent);
  box-shadow: 0 0 12px rgba(255, 107, 0, 0.3);
}

.validate-btn:disabled, .save-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
</style>
