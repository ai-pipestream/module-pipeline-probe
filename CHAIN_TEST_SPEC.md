# Chain Test Mode — Implementation Spec

## Overview

Add a **Chain Test** tab to the testing sidecar frontend alongside the existing single-module test. Chain test calls the engine's `ProcessNode` RPC sequentially for each step, using `node_override` to pass inline configs without requiring a loaded pipeline graph. The engine runs full semantics (CEL filters, pre/post mappings, module calls) but skips routing (`dry_run=true`).

## Backend: New endpoint

### `POST /test-sidecar/v1/run/chain`

```json
{
  "input_source": "sample",
  "sample_name": "alice_in_wonderland",
  "steps": [
    {
      "module_name": "parser",
      "module_config": { "enableTika": true },
      "pre_mappings": [],
      "post_mappings": [],
      "filter_conditions": []
    },
    {
      "module_name": "chunker",
      "module_config": {
        "algorithm": "token",
        "sourceField": "body",
        "chunkSize": 512,
        "chunkOverlap": 50
      },
      "pre_mappings": [],
      "post_mappings": [],
      "filter_conditions": []
    },
    {
      "module_name": "embedder",
      "module_config": {
        "embeddingModels": ["all-MiniLM-L6-v2"],
        "checkChunks": true
      },
      "pre_mappings": [],
      "post_mappings": [],
      "filter_conditions": []
    }
  ]
}
```

Input sources: `sample` (bundled samples), `upload` (file upload), `repository` (fetch from repo by doc_id).

### Response

```json
{
  "success": true,
  "total_duration_ms": 320,
  "steps": [
    {
      "step_index": 0,
      "module_name": "parser",
      "success": true,
      "duration_ms": 180,
      "processor_logs": [
        "Document received: alice.txt, 148576 bytes, MIME type: text/plain",
        "Parsed successfully: extracted 26847 words, title: 'Alice in Wonderland'",
        "Parsing completed in 178ms"
      ],
      "engine_logs": [
        "Processing node 'test-parser' (module: parser, pre_mappings: 0, post_mappings: 0, filters: 0)",
        "No filter conditions — document passes through",
        "Node 'test-parser' completed in 180ms",
        "No graph loaded — routing evaluation skipped (test mode with node_override)"
      ],
      "output_doc_summary": {
        "doc_id": "test-doc",
        "has_body": true,
        "body_length": 148576,
        "has_title": true,
        "semantic_results_count": 0
      }
    },
    {
      "step_index": 1,
      "module_name": "chunker",
      "success": true,
      "duration_ms": 45,
      "processor_logs": [
        "Chunking strategy: token with chunk size 512 and overlap 50",
        "Chunking document test-doc: source field 'body', text length: 148576 characters",
        "Produced 312 chunks from 148576 characters (avg chunk size: 476 chars)",
        "Chunking completed in 43ms"
      ],
      "engine_logs": [
        "Applied 0 pre-mapping(s) before module call",
        "Node 'test-chunker' completed in 45ms"
      ],
      "output_doc_summary": {
        "doc_id": "test-doc",
        "has_body": true,
        "body_length": 148576,
        "has_title": true,
        "semantic_results_count": 1,
        "total_chunks": 312
      }
    }
  ]
}
```

### Backend implementation

The chain endpoint loops over steps, calling the engine for each:

```java
PipeDoc currentDoc = resolveInputDoc(request);

List<ChainStepResult> results = new ArrayList<>();

for (int i = 0; i < request.steps().size(); i++) {
    ChainStep step = request.steps().get(i);

    // Build inline GraphNode with the step's config
    GraphNode nodeOverride = GraphNode.newBuilder()
        .setNodeId("test-" + step.moduleName() + "-" + i)
        .setModuleId(step.moduleName())
        .setCustomConfig(buildProcessConfig(step.moduleConfig()))
        .addAllPreMappings(step.preMappings())
        .addAllPostMappings(step.postMappings())
        .addAllFilterConditions(step.filterConditions())
        .build();

    // Build PipeStream with test flags
    PipeStream stream = PipeStream.newBuilder()
        .setStreamId("chain-test-" + UUID.randomUUID())
        .setMetadata(StreamMetadata.newBuilder()
            .setIsTest(true)
            .setDryRun(true))
        .setDocument(currentDoc)
        .setCurrentNodeId(nodeOverride.getNodeId())
        .build();

    // Call engine with node_override — full semantics, no routing
    ProcessNodeResponse response = engineClient.processNode(
        ProcessNodeRequest.newBuilder()
            .setStream(stream)
            .setNodeOverride(nodeOverride)
            .build()
    );

    // Extract logs from StepExecutionRecord history
    List<String> engineLogs = response.getUpdatedStream()
        .getMetadata().getHistoryList().stream()
        .flatMap(h -> h.getProcessorLogsList().stream())
        .toList();

    results.add(new ChainStepResult(i, step.moduleName(), response, engineLogs));

    // Chain: output doc becomes next step's input
    currentDoc = response.getUpdatedStream().getDocument();
}
```

### Key details

- **Engine connection**: The sidecar already has dynamic gRPC. Use `DynamicGrpcClientFactory` to call `EngineV1Service.ProcessNode` on the engine (service name: `pipestream-engine`).
- **`node_override`**: New field on `ProcessNodeRequest` (field 3). When `is_test=true` and `node_override` is present, engine bypasses graph cache and uses the inline `GraphNode` directly.
- **`dry_run=true`**: Engine runs full semantics (hydration, CEL, mappings, module call) but skips dispatch to downstream nodes.
- **Processing logs**: Two sources per step:
  1. **Module processor_logs** from `ProcessDataResponse` (audit trail from the module itself)
  2. **Engine processor_logs** from `StepExecutionRecord.processor_logs` in the stream's `metadata.history` (filter results, mapping counts, routing decisions)
- **Fixtures**: Each step's `output_doc` can optionally be saved as a fixture for reuse in single-module tests.

## Frontend: Chain Test tab

### Layout

Add a tab next to the existing single-module test:
- **Tab 1**: "Module Test" (existing single-module UI, unchanged)
- **Tab 2**: "Chain Test" (new)

### Chain Test UI

1. **Input Source** — same as existing: sample selector, file upload, or repository doc
2. **Step List** — ordered list of steps, each with:
   - Module selector (dropdown, same as existing module list)
   - Config editor (JSON textarea, same as existing)
   - Expandable: pre_mappings, post_mappings, filter_conditions (advanced, collapsed by default)
   - Add/remove step buttons, drag to reorder
3. **Run Chain** button
4. **Results** — accordion/timeline view showing each step:
   - Step header: module name, success/fail badge, duration
   - Expand to see: processor_logs, engine_logs, output_doc summary
   - "View full output" toggle for raw JSON tree (reuse existing JSON viewer)
   - "Save as fixture" button per step

### Preset chains

Include a few preset chain configurations:

| Preset | Steps |
|--------|-------|
| Parser → Chunker | parser (Tika) → chunker (token, body, 512) |
| Parser → Chunker → Embedder | parser → chunker → embedder (MiniLM) |
| Chunker → Embedder (pre-parsed) | chunker → embedder (uses sample with body pre-populated) |

## Proto references

- `ProcessNodeRequest.node_override` — field 3, type `ai.pipestream.config.v1.GraphNode`
- `StreamMetadata.is_test` — field 13
- `StreamMetadata.dry_run` — field 14
- `ProcessDataRequest.is_test` — field 6
- `ProcessDataRequest.dry_run` — field 7
- `StepExecutionRecord.processor_logs` — field 7 (engine processing logs)
- `ProcessDataResponse.processor_logs` — field 4 (module audit logs)

## What NOT to change

- Existing single-module test endpoints and UI — leave unchanged
- Module code — modules already handle `is_test` and produce `processor_logs`
- Engine code — all the wiring is already in place (node_override, dry_run, log collection)

---

## Design Decisions (Q&A)

These answers are authoritative — do not re-ask or deviate.

1. **Fail-fast or continue on step failure?** Fail-fast. If a step fails, stop the chain. Return partial results up to the failed step with the error. Running downstream steps after a failure is pointless.

2. **Repository input field?** Use the same `repository_node_id` field already used in single-module test. Don't introduce a new field name.

3. **Chain validation (legal step ordering, MIME expectations)?** No separate validation layer. Let engine/module errors be the gate. Modules already report clear errors via processor_logs. Over-validating upfront adds complexity for marginal benefit.

4. **Mixed input types (e.g., embedder as step 0)?** Allow it. If someone puts an embedder first with no chunks, they'll get "No semantic results found — skipping embedding (was chunker configured?)" in the audit log. That's the learning experience the sidecar is built for.

5. **sourceField / metadata expectations if parser doesn't produce body?** Not the sidecar's problem. That's what pre/post mappings are for — and why we run through the engine path. The chunker's audit log will say "No text found in source field 'body' — no chunks produced." Clear signal, no crash.

6. **Fixture persistence mode?** Explicit click only ("Save as fixture" per step). Don't auto-persist. Test runs are ephemeral by default.

7. **Fixture saving semantics / S3?** Don't persist to S3. Save to local/browser as a downloadable JSON artifact with step name in the filename. S3 fixture persistence is a future enhancement.

8. **Full output_doc per step?** Return summaries by default. Add an `include_full_output` boolean on the request. Full PipeDocs can be huge after parsing — don't send them over the wire unless asked.

9. **Top-level success semantics?** `success=false` if ANY step fails. Include `failed_at_step` index so the frontend can highlight which step broke.

10. **Service discovery fields per step?** `module_name` only. The engine resolves module_name → gRPC service name via the module registry. No need for per-step pipeline_name/stream_id/context overrides — those are engine-internal concerns.

11. **Config precedence?** Step-level inline config wins, full stop. No merge with schema defaults. The user explicitly chose their test config. Module-internal defaults (when JSON fields are absent) still apply naturally within the module.

12. **Log deduplication?** Keep both module logs and engine logs as-is, no dedup. They describe different things — module says "Produced 312 chunks", engine says "Applied 1 pre-mapping." Separate concerns, both valuable. Display them in separate sections in the UI.

13. **Max chain length / timeout?** 10 steps max, 5 minute timeout. Hard-code as constants. Don't expose in request/response. Nobody needs a 10-step chain — 3-4 is the realistic max.

14. **Payload shape for mappings?** Typed objects matching the proto JSON representation. Not raw strings. The frontend should send structured JSON that maps to `ProcessingMapping` proto shape. Same serialization the existing config editor uses.

15. **One endpoint or separate per input mode?** One endpoint (`/run/chain`). The `input_source` field discriminates. Same pattern as existing single-module test which already handles sample/upload/repository through one flow.
