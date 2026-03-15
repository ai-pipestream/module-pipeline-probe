<template>
  <main class="container">
    <header class="topbar">
      <h1>Module Testing Sidecar</h1>
      <p>Run module-specific tests from a registered module list. Choose from bundled sample documents, file uploads, or repository documents.</p>
    </header>

    <section class="card">
      <div class="mode-switch">
        <label>
          <input type="radio" value="module" v-model="activeTestMode" />
          Module Test
        </label>
        <label>
          <input type="radio" value="chain" v-model="activeTestMode" />
          Chain Test
        </label>
      </div>
    </section>

    <template v-if="activeTestMode === 'module'">
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
    </template>

    <template v-else>
      <section class="card">
        <h2>1) Chain input</h2>
        <div class="mode-switch">
          <label>
            <input type="radio" value="sample" v-model="chainInputMode" />
            Sample document
          </label>
          <label>
            <input type="radio" value="upload" v-model="chainInputMode" />
            Upload file
          </label>
          <label>
            <input type="radio" value="repository" v-model="chainInputMode" />
            Repository document
          </label>
        </div>

        <div v-if="chainInputMode === 'sample'" class="row">
          <label for="chain-sample-doc-select">Sample document</label>
          <select id="chain-sample-doc-select" v-model="chainSelectedSampleId">
            <option value="" disabled>Choose a sample file</option>
            <option
              v-for="sample in sampleDocuments"
              :key="sample.id"
              :value="sample.id"
            >
              {{ sample.title }} ({{ sample.mimeType.split('/')[1] }}) -- {{ formatBytes(sample.sizeBytes) }}
            </option>
          </select>
        </div>

        <div v-if="chainInputMode === 'upload'" class="row">
          <label for="chain-input-file">Input file</label>
          <input id="chain-input-file" type="file" @change="onChainFileSelected" />
          <p v-if="chainSelectedFile">Selected: {{ chainSelectedFile.name }}</p>
        </div>

        <div v-if="chainInputMode === 'repository'" class="row">
          <label for="chain-repository-doc-select">Repository document</label>
          <select id="chain-repository-doc-select" v-model="chainSelectedRepositoryNodeId">
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
        <h2>2) Chain steps</h2>
        <div class="row">
          <label for="chain-preset-select">Preset</label>
          <select id="chain-preset-select" v-model="selectedChainPreset" @change="applyChainPreset">
            <option value="" disabled>Choose a preset</option>
            <option
              v-for="preset in chainPresets"
              :key="preset.id"
              :value="preset.id"
            >
              {{ preset.name }}
            </option>
          </select>
          <button type="button" class="btn-secondary" @click="addChainStep">Add step</button>
        </div>

        <div
          v-for="(step, index) in chainSteps"
          :key="step.key"
          class="chain-step"
          draggable="true"
          @dragstart="onChainStepDragStart(index)"
          @dragover.prevent
          @drop="onChainStepDrop(index)"
        >
          <h3>Step {{ index + 1 }}</h3>
          <div class="chain-step-actions">
            <button type="button" class="btn-small" :disabled="index === 0" @click="moveChainStep(index, -1)">↑</button>
            <button type="button" class="btn-small" :disabled="index === chainSteps.length - 1" @click="moveChainStep(index, 1)">↓</button>
            <button type="button" class="btn-small btn-secondary" @click="removeChainStep(index)" :disabled="chainSteps.length <= 1">Remove</button>
          </div>

          <div class="row">
            <label :for="`chain-module-${index}`">Module</label>
            <select :id="`chain-module-${index}`" v-model="step.moduleName" @change="onChainModuleChanged(index)">
              <option value="" disabled>Select module</option>
              <option
                v-for="target in targets"
                :key="`${step.key}-${target.moduleName}`"
                :value="target.moduleName"
              >
                {{ target.displayName || target.moduleName }}
              </option>
            </select>
          </div>

          <label>Module config</label>
          <textarea rows="6" v-model="step.moduleConfigText"></textarea>

          <details class="chain-mappings">
            <summary>Advanced mappings</summary>
            <label>pre_mappings</label>
            <textarea rows="4" v-model="step.preMappingsText"></textarea>
            <label>post_mappings</label>
            <textarea rows="4" v-model="step.postMappingsText"></textarea>
            <label>filter_conditions</label>
            <textarea rows="4" v-model="step.filterConditionsText"></textarea>
          </details>
        </div>
      </section>

      <section class="card">
        <h2>3) Run chain</h2>
        <label class="inline-check">
          <input type="checkbox" v-model="chainIncludeFullOutput" />
          Include full output for each step
        </label>
        <div class="row">
          <button type="button" @click="runChain" :disabled="chainRunning || !canRunChain">{{ chainRunning ? 'Running...' : 'Run chain' }}</button>
          <button type="button" @click="clearChainResult" :disabled="!chainResult && !chainRunError">Clear result</button>
        </div>
        <p v-if="chainRunError" class="warning">{{ chainRunError }}</p>
      </section>

      <section class="card result-card" v-if="chainResult">
        <div class="result-header">
          <h2>Chain result</h2>
          <div class="result-actions">
            <button type="button" class="btn-small btn-secondary" @click="copyChainResult">{{ chainCopyLabel }}</button>
          </div>
        </div>
        <div class="result-summary">
          <span class="summary-chip"><strong>Success:</strong> {{ (chainResult.success ?? chainResult.is_success) ? 'yes' : 'no' }}</span>
          <span class="summary-chip"><strong>Duration:</strong> {{ chainResult.totalDurationMs ?? chainResult.total_duration_ms }}ms</span>
          <span class="summary-chip"><strong>Failed step:</strong> {{ (chainResult.failedAtStep ?? chainResult.failed_at_step) >= 0 ? ((chainResult.failedAtStep ?? chainResult.failed_at_step) + 1) : 'n/a' }}</span>
          <span class="summary-chip"><strong>Message:</strong> {{ chainResult.message || chainResult.error || '' }}</span>
        </div>
        <div class="chain-steps-timeline">
          <details v-for="step in (chainResult.steps || chainResult.chain_steps || [])" :key="`step-${step.stepIndex ?? step.step_index}`">
            <summary>
              Step {{ (step.stepIndex ?? step.step_index ?? 0) + 1 }} — {{ step.moduleName || step.module_name || 'unknown' }} —
              <span :class="(step.success ?? step.successful) ? 'status-success' : 'status-error'">
                {{ (step.success ?? step.successful) ? 'success' : 'failed' }}
              </span>
              ({{ step.durationMs ?? step.duration_ms }}ms)
            </summary>
            <div class="chain-step-detail">
              <div class="step-log-grid">
                <div>
                  <h4>Processor logs</h4>
                  <ul>
                    <li v-for="(log, logIndex) in (step.processorLogs || step.processor_logs || [])" :key="`step-${step.stepIndex ?? step.step_index}-processor-${logIndex}`">{{ log }}</li>
                  </ul>
                </div>
                <div>
                  <h4>Engine logs</h4>
                  <ul>
                    <li v-for="(log, logIndex) in (step.engineLogs || step.engine_logs || [])" :key="`step-${step.stepIndex ?? step.step_index}-engine-${logIndex}`">{{ log }}</li>
                  </ul>
                </div>
              </div>
              <div class="step-summary">
                <strong>Output summary:</strong>
                <VueJsonPretty
                  :data="step.outputDocSummary || step.output_doc_summary || {}"
                  :deep="3"
                  :showLength="true"
                  :showLine="false"
                  :showDoubleQuotes="true"
                  :showIcon="true"
                  :collapsedOnClickBrackets="true"
                />
              </div>
              <div class="row">
                <button type="button" class="btn-small" @click="toggleChainStepOutput(step.stepIndex ?? step.step_index)">
                  View / hide full output
                </button>
                <button
                  v-if="step.outputDoc || step.output_doc"
                  type="button"
                  class="btn-small btn-secondary"
                  @click="saveChainOutput(step.stepIndex ?? step.step_index)"
                >
                  Save as fixture
                </button>
              </div>
              <div v-if="chainOutputExpanded[step.stepIndex ?? step.step_index]" class="json-viewer-wrap">
                <div class="result-actions">
                  <label class="depth-control">
                    Expand depth
                    <input type="number" v-model.number="chainJsonDepth" min="1" max="20" />
                  </label>
                  <button type="button" class="btn-small" @click="chainJsonDepth = 1">Collapse</button>
                  <button type="button" class="btn-small" @click="chainJsonDepth = 10">Expand</button>
                </div>
                <VueJsonPretty
                  :data="compactVectors(step.output_doc || step.outputDoc)"
                  :deep="chainJsonDepth"
                  :showLength="true"
                  :showLine="false"
                  :showDoubleQuotes="true"
                  :showIcon="true"
                  :collapsedOnClickBrackets="true"
                />
              </div>
            </div>
          </details>
        </div>
      </section>
    </template>
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

const activeTestMode = ref('module')
const chainInputMode = ref('sample')
const chainSelectedSampleId = ref('')
const chainSelectedRepositoryNodeId = ref('')
const chainSelectedFile = ref(null)
const chainRunning = ref(false)
const chainRunError = ref('')
const chainResult = ref(null)
const chainIncludeFullOutput = ref(false)
const chainCopyLabel = ref('Copy chain JSON')
const chainOutputExpanded = ref({})
const chainJsonDepth = ref(3)
const chainDraggedIndex = ref(-1)

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
const chainPresetSeed = ref(1)

const chainSteps = ref([
  {
    key: `chain-step-${Date.now()}-${chainPresetSeed.value}`,
    moduleName: '',
    moduleConfigText: '{}',
    preMappingsText: '[]',
    postMappingsText: '[]',
    filterConditionsText: '[]'
  }
])
const selectedChainPreset = ref('')
const chainPresets = ref([
  {
    id: 'parser-chunker',
    name: 'Parser → Chunker',
    steps: [
      { moduleHint: 'parser', moduleConfig: { enableTika: true } },
      { moduleHint: 'chunker', moduleConfig: { chunkingMode: 'token', chunkSize: 512, chunkOverlap: 50 } }
    ]
  },
  {
    id: 'parser-chunker-embedder',
    name: 'Parser → Chunker → Embedder',
    steps: [
      { moduleHint: 'parser', moduleConfig: { enableTika: true } },
      { moduleHint: 'chunker', moduleConfig: { chunkingMode: 'token', chunkSize: 512, chunkOverlap: 50 } },
      { moduleHint: 'embedder', moduleConfig: {} }
    ]
  },
  {
    id: 'chunker-embedder',
    name: 'Chunker → Embedder',
    steps: [
      { moduleHint: 'chunker', moduleConfig: { chunkingMode: 'token', chunkSize: 512, chunkOverlap: 50 } },
      { moduleHint: 'embedder', moduleConfig: {} }
    ]
  },
  {
    id: 'parser-chunker-sentence-embedder',
    name: 'Parser → Sentence Chunker → Embedder',
    steps: [
      { moduleHint: 'parser', moduleConfig: { enableTika: true } },
      { moduleHint: 'chunker', moduleConfig: { chunkingMode: 'sentence', chunkSize: 120, chunkOverlap: 0 } },
      { moduleHint: 'embedder', moduleConfig: {} }
    ]
  }
])

const selectedSampleInfo = computed(() =>
  sampleDocuments.value.find((s) => s.id === selectedSampleId.value) || null
)

const chainSelectedSampleInfo = computed(() =>
  sampleDocuments.value.find((s) => s.id === chainSelectedSampleId.value) || null
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

const showUploadInput = computed(() => inputMode.value === 'upload')

const canRunChain = computed(() => {
  if (!chainSteps.value.length || chainRunning.value) {
    return false
  }

  const hasAllModules = chainSteps.value.every((step) => Boolean(step.moduleName))
  if (!hasAllModules) {
    return false
  }

  if (chainInputMode.value === 'upload') {
    return Boolean(chainSelectedFile.value) && Boolean(chainUploadBase64.value)
  }
  if (chainInputMode.value === 'sample') {
    return Boolean(chainSelectedSampleId.value)
  }
  if (chainInputMode.value === 'repository') {
    return Boolean(chainSelectedRepositoryNodeId.value)
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

const copyChainResult = async () => {
  try {
    const text = JSON.stringify(chainResult.value, null, 2)
    await navigator.clipboard.writeText(text)
    chainCopyLabel.value = 'Copied!'
    setTimeout(() => { chainCopyLabel.value = 'Copy chain JSON' }, 2000)
  } catch (_e) {
    chainCopyLabel.value = 'Copy failed'
    setTimeout(() => { chainCopyLabel.value = 'Copy chain JSON' }, 2000)
  }
}

const BODY_PREVIEW_CHARS = 500
const LONG_TEXT_FIELDS = new Set(['body', 'textContent', 'text_content', 'sourceText', 'source_text'])

/**
 * Recursively compact large data for display:
 * - Vector arrays (float[384]) become single-line preview strings
 * - Long text fields (body, textContent) are truncated with char count
 */
const compactVectors = (obj, parentKey = '') => {
  if (obj === null || obj === undefined) return obj
  if (Array.isArray(obj)) {
    if (obj.length > 10 && obj.every(v => typeof v === 'number')) {
      const preview = obj.slice(0, 5).map(v => v.toFixed(6)).join(', ')
      return `float[${obj.length}]: [${preview}, ...]`
    }
    return obj.map(item => compactVectors(item, ''))
  }
  if (typeof obj === 'string' && LONG_TEXT_FIELDS.has(parentKey) && obj.length > BODY_PREVIEW_CHARS) {
    return obj.substring(0, BODY_PREVIEW_CHARS) + `... (${obj.length.toLocaleString()} chars total)`
  }
  if (typeof obj === 'object') {
    const result = {}
    for (const [key, value] of Object.entries(obj)) {
      result[key] = compactVectors(value, key)
    }
    return result
  }
  return obj
}

const createChainStep = () => {
  const key = `chain-step-${Date.now()}-${chainPresetSeed.value}`
  chainPresetSeed.value += 1
  return {
    key,
    moduleName: '',
    moduleConfigText: '{}',
    preMappingsText: '[]',
    postMappingsText: '[]',
    filterConditionsText: '[]'
  }
}

const ensureChainModuleDefaults = (stepIndex) => {
  const step = chainSteps.value[stepIndex]
  if (!step || !step.moduleName) {
    return
  }
  const target = targets.value.find((entry) => entry.moduleName === step.moduleName)
  if (!target || !target.jsonConfigSchema) {
    if (step.moduleConfigText === '') {
      step.moduleConfigText = '{}'
    }
    return
  }

  let schema = null
  try {
    schema = JSON.parse(target.jsonConfigSchema)
  } catch (_err) {
    step.moduleConfigText = step.moduleConfigText || '{}'
    return
  }

  const defaults = deriveDefaultConfigFromSchema(schema)
  const mergedDefaults = ensureParserDefaults(defaults, target.parser)
  const mergedText = Object.keys(mergedDefaults).length > 0 ? JSON.stringify(mergedDefaults, null, 2) : '{}'
  step.moduleConfigText = mergedText
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

const chainUploadBase64 = ref('')

const onChainFileSelected = (event) => {
  const file = event?.target?.files?.[0] || null
  chainRunError.value = ''
  chainSelectedFile.value = file
  chainUploadBase64.value = ''
  if (!file) {
    return
  }

  const reader = new FileReader()
  reader.onload = () => {
    const result = typeof reader.result === 'string' ? reader.result : ''
    chainUploadBase64.value = result.split(',').pop() || ''
  }
  reader.onerror = () => {
    chainRunError.value = 'Failed to read chain upload file'
    chainSelectedFile.value = null
    chainUploadBase64.value = ''
  }
  reader.readAsDataURL(file)
}

const onRepositoryDocumentChanged = () => {
  if (!selectedRepositoryNodeId.value) {
    return
  }
  if (showUploadInput.value) {
    selectedFile.value = null
  }
}

const addChainStep = () => {
  chainSteps.value.push(createChainStep())
  selectedChainPreset.value = ''
}

const removeChainStep = (index) => {
  if (chainSteps.value.length <= 1) {
    return
  }
  chainSteps.value = chainSteps.value.filter((_, i) => i !== index)
}

const moveChainStep = (index, direction) => {
  const nextIndex = index + direction
  if (nextIndex < 0 || nextIndex >= chainSteps.value.length) {
    return
  }
  const working = [...chainSteps.value]
  const [item] = working.splice(index, 1)
  if (!item) {
    return
  }
  working.splice(nextIndex, 0, item)
  chainSteps.value = working
}

const onChainStepDragStart = (index) => {
  chainDraggedIndex.value = index
}

const onChainStepDrop = (dropIndex) => {
  const from = chainDraggedIndex.value
  if (from < 0 || from === dropIndex) {
    chainDraggedIndex.value = -1
    return
  }
  const working = [...chainSteps.value]
  const [item] = working.splice(from, 1)
  if (!item) {
    chainDraggedIndex.value = -1
    return
  }
  working.splice(dropIndex, 0, item)
  chainSteps.value = working
  chainDraggedIndex.value = -1
}

const onChainModuleChanged = (index) => {
  ensureChainModuleDefaults(index)
}

const applyChainPreset = () => {
  const preset = chainPresets.value.find((candidate) => candidate.id === selectedChainPreset.value)
  if (!preset) {
    return
  }

  const next = []
  preset.steps.forEach((stepHint) => {
    const moduleHint = (stepHint.moduleHint || '').toLowerCase()
    const target = targets.value.find((target) =>
      (target.moduleName || '').toLowerCase().includes(moduleHint)
    )
    const moduleName = target?.moduleName || ''
    const created = createChainStep()
    created.moduleName = moduleName
    const initialConfig = stepHint.moduleConfig && typeof stepHint.moduleConfig === 'object'
      ? stepHint.moduleConfig
      : {}

    if (target?.jsonConfigSchema) {
      try {
        const schema = JSON.parse(target.jsonConfigSchema)
        const defaults = deriveDefaultConfigFromSchema(schema)
        const merged = { ...defaults, ...initialConfig }
        created.moduleConfigText = JSON.stringify(ensureParserDefaults(merged, target.parser), null, 2)
      } catch (_err) {
        created.moduleConfigText = JSON.stringify(initialConfig, null, 2)
      }
    } else {
      created.moduleConfigText = JSON.stringify(initialConfig, null, 2)
    }

    next.push(created)
  })

  if (next.length > 0) {
    chainSteps.value = next
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

const parseChainMappings = (rawValue, type) => {
  if (!rawValue || !rawValue.trim()) {
    return []
  }
  let parsed = null
  try {
    parsed = JSON.parse(rawValue)
  } catch (_err) {
    throw new Error(`Invalid ${type} JSON array`)
  }
  if (!Array.isArray(parsed)) {
    throw new Error(`Invalid ${type} JSON array`)
  }
  return parsed
}

const parseChainConfig = (rawText, stepIndex) => {
  if (!rawText || !rawText.trim()) {
    return {}
  }
  let value = null
  try {
    value = JSON.parse(rawText)
  } catch (_err) {
    throw new Error(`Invalid JSON in step ${stepIndex + 1} module_config`)
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`Invalid JSON object in step ${stepIndex + 1} module_config`)
  }
  return value
}

const runChain = async () => {
  if (!canRunChain.value) {
    return
  }

  chainRunning.value = true
  chainRunError.value = ''
  chainResult.value = null
  chainOutputExpanded.value = {}

  try {
    const steps = chainSteps.value.map((step, index) => {
      const moduleConfig = parseChainConfig(step.moduleConfigText || '{}', index)
      const preMappings = parseChainMappings(step.preMappingsText || '[]', `step ${index + 1} pre_mappings`)
      const postMappings = parseChainMappings(step.postMappingsText || '[]', `step ${index + 1} post_mappings`)
      const filterConditions = parseChainMappings(step.filterConditionsText || '[]', `step ${index + 1} filter_conditions`)

      return {
        moduleName: step.moduleName,
        moduleConfig,
        preMappings,
        postMappings,
        filterConditions
      }
    })

    const request = {
      inputSource: chainInputMode.value,
      steps,
      includeFullOutput: chainIncludeFullOutput.value,
      accountId: ''
    }

    if (chainInputMode.value === 'sample') {
      request.sampleId = chainSelectedSampleId.value
      request.sampleName = chainSelectedSampleInfo.value?.fileName || chainSelectedSampleInfo.value?.name || ''
    } else if (chainInputMode.value === 'upload') {
      if (!chainSelectedFile.value || !chainUploadBase64.value) {
        throw new Error('Upload requires a file')
      }
      request.upload = {
        filename: chainSelectedFile.value.name || 'upload.bin',
        mimeType: chainSelectedFile.value.type || 'application/octet-stream',
        base64Data: chainUploadBase64.value
      }
    } else if (chainInputMode.value === 'repository') {
      const doc = repositoryDocuments.value.find((item) => item.nodeId === chainSelectedRepositoryNodeId.value)
      if (!doc) {
        throw new Error('Select a repository document first')
      }
      request.repositoryNodeId = doc.nodeId
      request.repositoryDrive = doc.drive || ''
      request.repositoryHydrateBlobFromStorage = false
    }

    const response = await fetch(`${API_BASE}/run/chain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request)
    })

    const responseText = await response.text()
    let parsed = null
    try {
      parsed = responseText ? JSON.parse(responseText) : null
    } catch (_err) {
      parsed = { message: responseText || 'No response body' }
    }

    if (!response.ok) {
      const errMsg = parsed?.message || parsed?.error || responseText || `HTTP ${response.status}`
      throw new Error(errMsg)
    }

    chainResult.value = parsed
  } catch (errorObj) {
    chainRunError.value = errorObj?.message || 'Chain run failed'
  } finally {
    chainRunning.value = false
  }
}

const clearChainResult = () => {
  chainResult.value = null
  chainRunError.value = ''
  chainOutputExpanded.value = {}
}

const getChainSteps = () => (chainResult.value?.steps || chainResult.value?.chain_steps || [])

const toggleChainStepOutput = (stepIndex) => {
  chainOutputExpanded.value = {
    ...chainOutputExpanded.value,
    [stepIndex]: !chainOutputExpanded.value[stepIndex]
  }
}

const saveChainOutput = (stepIndex) => {
  const steps = getChainSteps()
  if (!steps?.[stepIndex]) {
    return
  }
  const step = steps[stepIndex]
  const safeName = (step.moduleName || step.module_name || `step-${stepIndex + 1}`).replace(/[^a-z0-9-_]+/gi, '-')
  const filename = `fixture-${safeName}-${stepIndex + 1}.json`
  const data = JSON.stringify(step.output_doc || step.outputDoc || step.output_doc_summary || step.outputDocSummary || {}, null, 2)
  const url = URL.createObjectURL(new Blob([data], { type: 'application/json' }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  anchor.click()
  URL.revokeObjectURL(url)
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

.inline-check {
  display: inline-flex;
  align-items: center;
  gap: 6px;
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

.chain-step {
  border: 1px dashed #c5c5c5;
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 10px;
  background: #fafcff;
}

.chain-step-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.chain-mappings {
  margin-top: 10px;
}

.chain-mappings label {
  display: block;
  margin: 8px 0 4px;
}

.chain-step h3 {
  margin: 0 0 10px;
}

.chain-steps-timeline {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.chain-step-detail {
  margin-top: 8px;
  padding: 8px 10px;
  border: 1px solid #e4e4e4;
  border-radius: 6px;
  background: #fafafa;
}

.step-log-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
}

.step-log-grid ul {
  margin: 6px 0 0;
  padding-left: 18px;
  font-size: 13px;
}

.step-summary {
  margin-top: 12px;
}

.status-success {
  color: #0b8043;
  font-weight: 600;
}

.status-error {
  color: #b00020;
  font-weight: 600;
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
