<template>
  <main class="container">
    <header class="topbar">
      <h1>Module Testing Sidecar</h1>
      <p>Run module-specific tests from a registered module list. Choose from bundled sample documents, file uploads, or repository documents.</p>
    </header>

    <section class="card">
      <h2>1) Select a module</h2>
      <div class="row">
        <label for="module-select">Target module</label>
        <select id="module-select" v-model="selectedModuleName" @change="onModuleChanged" :disabled="loadingTargets || running">
          <option value="" disabled>Select module</option>
          <option
            v-for="target in targets"
            :key="target.serviceId || target.moduleName"
            :value="target.moduleName"
          >
            {{ target.displayName || target.moduleName }} ({{ target.parser ? 'parser' : 'non-parser' }})
          </option>
        </select>
      </div>

      <div v-if="selectedTarget" class="meta">
        <span>Version: {{ selectedTarget.version || 'n/a' }}</span>
        <span>Module: {{ selectedTarget.moduleName }}</span>
        <span>Healthy: {{ selectedTarget.healthy ? 'yes' : 'no' }}</span>
      </div>
      <div v-if="selectedTarget && selectedTarget.registrationError" class="meta warning">
        {{ selectedTarget.registrationError }}
      </div>
    </section>

    <section class="card" v-if="selectedTarget">
      <h2>2) Choose input</h2>

      <div class="mode-switch">
        <label v-if="selectedTarget.parser">
          <input type="radio" value="upload" v-model="inputMode" />
          Upload file
        </label>
        <label>
          <input type="radio" value="sample" v-model="inputMode" />
          Sample document
        </label>
        <label>
          <input type="radio" value="repository" v-model="inputMode" />
          Repository document
        </label>
      </div>

      <div v-if="inputMode === 'upload' && selectedTarget.parser" class="row">
        <label for="input-file">Input file</label>
        <input
          id="input-file"
          type="file"
          @change="onFileSelected"
        />
        <p v-if="selectedFile">Selected: {{ selectedFile.name }}</p>
      </div>

      <div v-if="inputMode === 'sample'" class="row">
        <label for="sample-doc-select">Sample document</label>
        <select
          id="sample-doc-select"
          v-model="selectedSampleId"
        >
          <option value="" disabled>Choose a sample file</option>
          <option
            v-for="sample in sampleDocuments"
            :key="sample.id"
            :value="sample.id"
          >
            {{ sample.title }} ({{ sample.mimeType.split('/')[1] }}) -- {{ formatBytes(sample.sizeBytes) }}
          </option>
        </select>
        <p v-if="selectedSampleInfo" class="muted">{{ selectedSampleInfo.description }}</p>
      </div>

      <div v-if="inputMode === 'repository'" class="row">
        <label for="repository-doc-select">Repository document</label>
        <select
          id="repository-doc-select"
          v-model="selectedRepositoryNodeId"
          @change="onRepositoryDocumentChanged"
        >
          <option value="" disabled>Choose document</option>
          <option
            v-for="doc in sortedRepositoryDocuments"
            :key="doc.nodeId"
            :value="doc.nodeId"
          >
            {{ doc.label }}
          </option>
        </select>
        <button type="button" @click="refreshRepositoryDocuments">Refresh documents</button>
      </div>
    </section>

    <section class="card">
      <h2>3) Module config</h2>
      <div v-if="selectedTarget?.parser" class="parser-engine-options">
        <h3>Parser engine options</h3>
        <label>
          <input
            type="checkbox"
            :checked="parserEngineFlags.enableTika"
            @change="setParserConfigValue('enableTika', $event.target.checked)"
          />
          Enable Tika
        </label>
        <label>
          <input
            type="checkbox"
            :checked="parserEngineFlags.enableDocling"
            @change="setParserConfigValue('enableDocling', $event.target.checked)"
          />
          Enable Docling
        </label>
      </div>
      <p class="muted">The target provides a JSON schema; you can edit the JSON below. A malformed JSON payload is rejected before calling the API.</p>
      <textarea
        v-model="moduleConfigText"
        placeholder='{"key":"value"}'
        rows="10"
      ></textarea>
      <div v-if="schemaError" class="warning">Schema load error: {{ schemaError }}</div>
    </section>

    <section class="card">
      <h2>4) Run</h2>
      <div class="row">
        <button
          type="button"
          @click="runTest"
          :disabled="running || !canRun"
        >
          {{ running ? 'Running...' : 'Run test' }}
        </button>
        <button type="button" @click="clearRunResult" :disabled="!runResult && !runError">Clear result</button>
        <button type="button" @click="fetchLastError" class="btn-secondary">View last server error</button>
      </div>
      <p v-if="runError" class="warning">{{ runError }}</p>
    </section>

    <section class="card result-card" v-if="runResult">
      <div class="result-header">
        <h2>Result</h2>
        <div class="result-actions">
          <label class="depth-control">
            Expand depth
            <input type="number" v-model.number="jsonDepth" min="1" max="20" />
          </label>
          <button type="button" class="btn-small" @click="jsonDepth = 1">Collapse</button>
          <button type="button" class="btn-small" @click="jsonDepth = 20">Expand all</button>
          <button type="button" class="btn-small btn-secondary" @click="copyResult">{{ copyLabel }}</button>
        </div>
      </div>
      <div v-if="resultSummary" class="result-summary">
        <span v-for="(val, key) in resultSummary" :key="key" class="summary-chip">
          <strong>{{ key }}:</strong> {{ val }}
        </span>
      </div>
      <div class="json-viewer-wrap">
        <VueJsonPretty
          :data="runResult"
          :deep="jsonDepth"
          :showLength="true"
          :showLine="false"
          :showDoubleQuotes="true"
          :showIcon="true"
          :collapsedOnClickBrackets="true"
        />
      </div>
    </section>
  </main>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import VueJsonPretty from 'vue-json-pretty'
import 'vue-json-pretty/lib/styles.css'

const API_BASE = (() => {
  const path = window.location.pathname || '/'
  const adminIndex = path.indexOf('/admin')
  if (adminIndex < 0) {
    return '/test-sidecar/v1'
  }
  const root = path.substring(0, adminIndex)
  return `${root}/test-sidecar/v1`.replace(/\/+/g, '/')
})()

const targets = ref([])
const selectedModuleName = ref('')
const selectedTarget = ref(null)
const loadingTargets = ref(false)
const loadingDocuments = ref(false)
const repositoryDocuments = ref([])
const sortedRepositoryDocuments = computed(() =>
  [...repositoryDocuments.value].sort((a, b) => (a.label || '').localeCompare(b.label || ''))
)
const selectedRepositoryNodeId = ref('')
const sampleDocuments = ref([])
const selectedSampleId = ref('')
const inputMode = ref('sample')
const selectedFile = ref(null)
const moduleConfigText = ref('{}')
const running = ref(false)
const schemaError = ref('')
const runError = ref('')
const runResult = ref(null)
const error = ref('')
const statusMessage = ref('')
const jsonDepth = ref(3)
const copyLabel = ref('Copy JSON')

const selectedSampleInfo = computed(() =>
  sampleDocuments.value.find((s) => s.id === selectedSampleId.value) || null
)

const canRun = computed(() => {
  if (!selectedTarget.value || running.value) {
    return false
  }

  if (inputMode.value === 'upload') {
    return Boolean(selectedFile.value)
  }
  if (inputMode.value === 'sample') {
    return Boolean(selectedSampleId.value)
  }
  if (inputMode.value === 'repository') {
    return Boolean(selectedRepositoryNodeId.value)
  }

  return false
})

const effectiveMode = computed(() => inputMode.value)

const resultSummary = computed(() => {
  const r = runResult.value
  if (!r || typeof r !== 'object') return null
  const summary = {}
  if (r.success !== undefined) summary['Status'] = r.success ? 'Success' : 'Failed'
  if (r.processingTimeMs ?? r.processing_time_ms)
    summary['Time'] = `${r.processingTimeMs ?? r.processing_time_ms}ms`
  if (r.moduleName ?? r.module_name)
    summary['Module'] = r.moduleName ?? r.module_name
  const outDoc = r.outputDoc ?? r.output_doc
  if (outDoc) {
    const title = outDoc.title || outDoc.docId || outDoc.doc_id || ''
    if (title) summary['Doc'] = title
    const metaCount = Object.keys(outDoc.metadata ?? outDoc.structured_data ?? {}).length
    if (metaCount > 0) summary['Metadata fields'] = metaCount
  }
  if (r.error) summary['Error'] = r.error
  return Object.keys(summary).length > 0 ? summary : null
})

const copyResult = async () => {
  try {
    const text = JSON.stringify(runResult.value, null, 2)
    await navigator.clipboard.writeText(text)
    copyLabel.value = 'Copied!'
    setTimeout(() => { copyLabel.value = 'Copy JSON' }, 2000)
  } catch (_e) {
    copyLabel.value = 'Copy failed'
    setTimeout(() => { copyLabel.value = 'Copy JSON' }, 2000)
  }
}

const deriveDefaultConfigFromSchema = (schema) => {
  if (!schema || typeof schema !== 'object' || schema.type !== 'object') {
    return {}
  }

  if (schema.examples && Array.isArray(schema.examples) && schema.examples.length > 0) {
    const exampleValue = schema.examples[0]
    if (exampleValue && typeof exampleValue === 'object' && !Array.isArray(exampleValue)) {
      return { ...exampleValue }
    }
    if (typeof exampleValue === 'string') {
      try {
        const parsedExample = JSON.parse(exampleValue)
        if (parsedExample && typeof parsedExample === 'object' && !Array.isArray(parsedExample)) {
          return parsedExample
        }
      } catch (_err) {
        // Ignore non-object examples and continue to property-level defaults
      }
    }
  }

  if (!schema.properties) {
    return {}
  }

  const result = {}
  Object.entries(schema.properties).forEach(([key, prop]) => {
    if (!prop || typeof prop !== 'object') return
    if (Object.prototype.hasOwnProperty.call(prop, 'default')) {
      result[key] = prop.default
    } else if (prop.examples && Array.isArray(prop.examples) && prop.examples.length > 0) {
      result[key] = prop.examples[0]
    }
  })

  return result
}

const ensureParserDefaults = (defaults, isParserModule) => {
  if (!isParserModule || !defaults || typeof defaults !== 'object' || Array.isArray(defaults)) {
    return defaults
  }

  const merged = { ...defaults }
  if (!Object.prototype.hasOwnProperty.call(merged, 'enableTika')) {
    merged.enableTika = true
  }
  if (!Object.prototype.hasOwnProperty.call(merged, 'enableDocling')) {
    merged.enableDocling = false
  }
  return merged
}

const parserConfig = computed(() => {
  if (!selectedTarget.value?.parser) {
    return {}
  }
  try {
    const parsed = parseModuleConfig()
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch (_err) {
    return {}
  }
})

const parserEngineFlags = computed(() => ({
  enableTika: parserConfig.value.enableTika !== undefined ? Boolean(parserConfig.value.enableTika) : true,
  enableDocling: parserConfig.value.enableDocling !== undefined ? Boolean(parserConfig.value.enableDocling) : false
}))

const setParserConfigValue = (key, value) => {
  let current = {}
  try {
    const parsed = parseModuleConfig()
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      current = { ...parsed }
    }
  } catch (_err) {
    current = {}
  }
  current[key] = value
  moduleConfigText.value = JSON.stringify(current, null, 2)
}

const loadTargets = async () => {
  loadingTargets.value = true
  error.value = ''
  try {
    const response = await fetch(`${API_BASE}/targets`)
    if (!response.ok) {
      throw new Error(`Failed loading modules: HTTP ${response.status}`)
    }
    const json = await response.json()
    const list = Array.isArray(json) ? json : (json.targets || [])
    targets.value = list.filter(Boolean)
    if (!selectedModuleName.value && targets.value.length > 0) {
      selectedModuleName.value = targets.value[0].moduleName || ''
      await onModuleChanged()
    }
  } catch (e) {
    error.value = e.message || 'Could not load modules'
  } finally {
    loadingTargets.value = false
  }
}

const loadSamples = async () => {
  try {
    const response = await fetch(`${API_BASE}/samples`)
    if (!response.ok) {
      throw new Error(`Failed loading samples: HTTP ${response.status}`)
    }
    sampleDocuments.value = await response.json()
  } catch (e) {
    statusMessage.value = e.message || 'Failed to load sample documents'
  }
}

const refreshRepositoryDocuments = async () => {
  loadingDocuments.value = true
  try {
    const response = await fetch(`${API_BASE}/repository/documents?limit=100`)
    if (!response.ok) {
      throw new Error(`Failed loading documents: HTTP ${response.status}`)
    }
    const json = await response.json()
    const rawDocuments = json?.documents || []

    repositoryDocuments.value = rawDocuments
      .filter(Boolean)
      .map((doc) => {
        const nodeId = doc.nodeId || doc.node_id || ''
        const docId = doc.docId || doc.doc_id || ''
        const title = doc.title || doc.doc_id || doc.nodeId || doc.node_id || nodeId || 'Document'
        const size = doc.sizeBytes ?? doc.size_bytes ?? 0
        const drive = doc.drive || ''
        return {
          nodeId,
          docId,
          title,
          label: `${title} (${docId || nodeId})${drive ? ` • drive=${drive}` : ''} • ${formatBytes(size)}`
        }
      })
      .filter((doc) => doc.nodeId)
  } catch (e) {
    statusMessage.value = e.message || 'Failed to load repository documents'
  } finally {
    loadingDocuments.value = false
  }
}

const parseModuleConfig = () => {
  if (!moduleConfigText.value || !moduleConfigText.value.trim()) {
    return {}
  }
  return JSON.parse(moduleConfigText.value)
}

const applyTargetSchemaDefaults = (target) => {
  schemaError.value = ''
  if (!target?.jsonConfigSchema) {
    moduleConfigText.value = '{}'
    return
  }

  let schema = null
  try {
    schema = JSON.parse(target.jsonConfigSchema)
  } catch (_err) {
    schemaError.value = 'Module schema is not valid JSON'
    moduleConfigText.value = '{}'
    return
  }

  const defaults = deriveDefaultConfigFromSchema(schema)
  const mergedDefaults = ensureParserDefaults(defaults, target.parser)
  moduleConfigText.value = Object.keys(mergedDefaults).length > 0
    ? JSON.stringify(mergedDefaults, null, 2)
    : '{}'
}

const onModuleChanged = async () => {
  runError.value = ''
  runResult.value = null
  selectedTarget.value = targets.value.find((t) => t.moduleName === selectedModuleName.value) || null

  if (!selectedTarget.value) {
    return
  }

  inputMode.value = 'sample'

  selectedFile.value = null
  selectedSampleId.value = ''
  selectedRepositoryNodeId.value = ''
  applyTargetSchemaDefaults(selectedTarget.value)
}

const onFileSelected = (event) => {
  const file = event?.target?.files?.[0] || null
  selectedFile.value = file
  if (file) {
    selectedRepositoryNodeId.value = ''
  }
}

const onRepositoryDocumentChanged = () => {
  if (!selectedRepositoryNodeId.value) {
    return
  }
  if (showUploadInput.value) {
    selectedFile.value = null
  }
}

const runTest = async () => {
  if (!selectedTarget.value || !canRun.value) {
    return
  }

  runError.value = ''
  runResult.value = null
  running.value = true

  try {
    const moduleConfig = parseModuleConfig()
    const moduleConfigPayload = JSON.stringify(moduleConfig || {})
    const commonRequest = {
      moduleName: selectedTarget.value.moduleName,
      includeOutputDoc: true,
      moduleConfig: moduleConfigPayload,
      accountId: '',
      pipelineName: 'module-testing-sidecar',
      pipeStepName: 'module-testing-step',
      streamId: '',
      currentHopNumber: 1,
      contextParams: {}
    }

    let response
    if (effectiveMode.value === 'upload') {
      const body = new FormData()
      body.append('moduleName', commonRequest.moduleName)
      body.append('accountId', commonRequest.accountId)
      body.append('includeOutputDoc', 'true')
      body.append('moduleConfigJson', commonRequest.moduleConfig)
      body.append('pipelineName', commonRequest.pipelineName)
      body.append('pipeStepName', commonRequest.pipeStepName)
      body.append('streamId', commonRequest.streamId)
      body.append('currentHopNumber', String(commonRequest.currentHopNumber))
      body.append('contextParamsJson', '{}')
      body.append('file', selectedFile.value)

      response = await fetch(`${API_BASE}/run/upload`, {
        method: 'POST',
        body
      })
    } else if (effectiveMode.value === 'sample') {
      if (!selectedSampleId.value) {
        throw new Error('Select a sample document first')
      }

      response = await fetch(`${API_BASE}/run/sample`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sampleId: selectedSampleId.value,
          moduleName: commonRequest.moduleName,
          moduleConfig: commonRequest.moduleConfig,
          includeOutputDoc: commonRequest.includeOutputDoc,
          accountId: commonRequest.accountId,
          pipelineName: commonRequest.pipelineName,
          pipeStepName: commonRequest.pipeStepName,
          streamId: commonRequest.streamId,
          currentHopNumber: commonRequest.currentHopNumber,
          contextParams: {}
        })
      })
    } else {
      const doc = repositoryDocuments.value.find((item) => item.nodeId === selectedRepositoryNodeId.value)
      if (!doc) {
        throw new Error('Select a repository document first')
      }

      response = await fetch(`${API_BASE}/run/repository`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          moduleName: commonRequest.moduleName,
          repositoryNodeId: doc.nodeId,
          drive: doc.drive || '',
          hydrateBlobFromStorage: false,
          moduleConfig: commonRequest.moduleConfig,
          includeOutputDoc: commonRequest.includeOutputDoc,
          accountId: commonRequest.accountId,
          pipelineName: commonRequest.pipelineName,
          pipeStepName: commonRequest.pipeStepName,
          streamId: commonRequest.streamId,
          currentHopNumber: 1,
          contextParams: {}
        })
      })
    }

    const responseText = await response.text()
    let parsed
    try {
      parsed = responseText ? JSON.parse(responseText) : null
    } catch (_err) {
      parsed = { message: responseText || 'No response body' }
    }

    if (!response.ok) {
      const errMsg = parsed?.message || parsed?.error || responseText || `HTTP ${response.status}`
      throw new Error(errMsg)
    }

    runResult.value = parsed
  } catch (errorObj) {
    runError.value = errorObj?.message || 'Module run failed'
  } finally {
    running.value = false
  }
}

const clearRunResult = () => {
  runResult.value = null
  runError.value = ''
}

const fetchLastError = async () => {
  try {
    const response = await fetch(`${API_BASE}/debug/last-error`)
    const json = await response.json()
    runResult.value = json
    runError.value = ''
  } catch (e) {
    runError.value = e.message || 'Failed to fetch last error'
  }
}

const formatBytes = (bytes) => {
  const value = Number(bytes || 0)
  if (value === 0) {
    return '0 B'
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const exponent = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1)
  const amount = value / Math.pow(1024, exponent)
  return `${amount.toFixed(exponent === 0 ? 0 : 1)} ${units[exponent]}`
}

onMounted(async () => {
  await Promise.all([
    loadTargets(),
    loadSamples(),
    refreshRepositoryDocuments()
  ])
})
</script>

<style scoped>
.container {
  font-family: Inter, Arial, sans-serif;
  max-width: 960px;
  margin: 0 auto;
  padding: 16px;
}

.topbar {
  margin-bottom: 16px;
}

.topbar h1 {
  margin: 0 0 8px;
}

.card {
  border: 1px solid #d8d8d8;
  border-radius: 8px;
  padding: 16px;
  margin-bottom: 12px;
  background: #ffffff;
}

.row {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 12px;
}

select,
input[type='file'],
textarea,
button {
  font-size: 14px;
}

select,
input[type='text'],
input[type='file'],
textarea {
  padding: 8px;
  border: 1px solid #b9b9b9;
  border-radius: 6px;
}

textarea {
  width: 100%;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}

button {
  border: 1px solid #3f51b5;
  background: #3f51b5;
  color: #fff;
  padding: 8px 14px;
  border-radius: 6px;
  cursor: pointer;
}

button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.btn-secondary {
  background: #757575;
  border-color: #757575;
}

.mode-switch {
  display: flex;
  gap: 16px;
  margin-bottom: 10px;
}

.meta {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  margin-top: 8px;
  font-size: 14px;
}

.parser-engine-options {
  margin-bottom: 10px;
}

.parser-engine-options h3 {
  margin: 0 0 8px;
}

.parser-engine-options label {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-right: 16px;
}

.warning {
  color: #b00020;
}

.muted {
  color: #5b5b5b;
  font-size: 13px;
}

.result-card {
  max-height: 80vh;
  display: flex;
  flex-direction: column;
}

.result-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 8px;
}

.result-header h2 {
  margin: 0;
}

.result-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.depth-control {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 13px;
}

.depth-control input {
  width: 48px;
  padding: 4px 6px;
  border: 1px solid #b9b9b9;
  border-radius: 4px;
  font-size: 13px;
  text-align: center;
}

.btn-small {
  font-size: 12px;
  padding: 4px 10px;
}

.result-summary {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  padding: 8px 12px;
  background: #f0f4ff;
  border-radius: 6px;
  margin-bottom: 10px;
  font-size: 13px;
}

.summary-chip strong {
  color: #3f51b5;
}

.json-viewer-wrap {
  overflow: auto;
  flex: 1;
  min-height: 0;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  padding: 8px;
  background: #fafafa;
}

.json-viewer-wrap :deep(.vjs-tree) {
  font-size: 13px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}
</style>
