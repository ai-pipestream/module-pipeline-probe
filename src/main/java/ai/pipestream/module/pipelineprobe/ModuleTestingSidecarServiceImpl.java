package ai.pipestream.module.pipelineprobe;

import ai.pipestream.data.module.v1.CapabilityType;
import ai.pipestream.data.module.v1.GetServiceRegistrationRequest;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.data.module.v1.MutinyPipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ServiceMetadata;
import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.ProcessingMapping;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.StreamMetadata;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.engine.v1.MutinyEngineV1ServiceGrpc;
import ai.pipestream.engine.v1.ProcessNodeRequest;
import ai.pipestream.engine.v1.ProcessNodeResponse;
import ai.pipestream.platform.registration.v1.GetModuleResponse;
import ai.pipestream.platform.registration.v1.ListPlatformModulesRequest;
import ai.pipestream.platform.registration.v1.ListPlatformModulesResponse;
import ai.pipestream.platform.registration.v1.MutinyPlatformRegistrationServiceGrpc;
import ai.pipestream.repository.pipedoc.v1.GetBlobRequest;
import ai.pipestream.repository.pipedoc.v1.GetBlobResponse;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocResponse;
import ai.pipestream.repository.pipedoc.v1.ListPipeDocsRequest;
import ai.pipestream.repository.pipedoc.v1.ListPipeDocsResponse;
import ai.pipestream.repository.pipedoc.v1.MutinyPipeDocServiceGrpc;
import ai.pipestream.repository.pipedoc.v1.PipeDocMetadata;
import ai.pipestream.testing.harness.v1.ModuleTestingSidecarService;
import ai.pipestream.testing.harness.v1.RepositoryInput;
import ai.pipestream.testing.harness.v1.RunModuleTestRequest;
import ai.pipestream.testing.harness.v1.RunModuleTestResponse;
import ai.pipestream.testing.harness.v1.ServiceContext;
import ai.pipestream.testing.harness.v1.UploadInput;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * gRPC-backed service implementation for module-testing-sidecar.
 */
@GrpcService
@Singleton
public class ModuleTestingSidecarServiceImpl implements ModuleTestingSidecarService {

    private static final Logger LOG = Logger.getLogger(ModuleTestingSidecarServiceImpl.class);
    private static final Set<CapabilityType> PARSER_CAPABILITIES = Set.of(CapabilityType.CAPABILITY_TYPE_PARSER);

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    @ConfigProperty(name = "module.testing.sidecar.platform-registration-service", defaultValue = "platform-registration")
    String platformRegistrationServiceName;

    @ConfigProperty(name = "module.testing.sidecar.repository-service", defaultValue = "repository")
    String repositoryServiceName;

    @ConfigProperty(name = "module.testing.sidecar.default-pipeline-name", defaultValue = "module-testing-sidecar")
    String defaultPipelineName;

    @ConfigProperty(name = "module.testing.sidecar.default-step-name", defaultValue = "module-testing-step")
    String defaultStepName;

    @ConfigProperty(name = "module.testing.sidecar.engine-service", defaultValue = "engine")
    String engineServiceName;

    private static final int MAX_CHAIN_STEPS = 10;
    private static final Duration CHAIN_TEST_TIMEOUT = Duration.ofMinutes(5);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Uni<RunModuleTestResponse> runModuleTest(RunModuleTestRequest request) {
        long startedAtMs = System.currentTimeMillis();
        Timestamp startedAt = toTimestamp(startedAtMs);

        return validateRequest(request)
            .chain(ignore -> validateUploadCapability(request))
            .chain(ignore -> resolveInputDocument(request))
            .flatMap(document -> executeModuleCall(request, document))
            .map(response -> buildRunResponse(request, response, startedAtMs, startedAt))
            .onFailure().recoverWithItem(throwable ->
                buildRunResponseFromFailure(request == null ? "unknown" : request.getModuleName(), throwable, startedAtMs, startedAt)
            );
    }

    public Uni<RunModuleTestResponse> runModuleTestWithPipeDoc(
            String moduleName,
            Struct moduleConfig,
            String accountId,
            ServiceContext context,
            PipeDoc inputDocument,
            boolean includeOutputDoc
    ) {
        if (moduleName == null || moduleName.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("module_name is required"));
        }
        if (inputDocument == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input document is required"));
        }

        long startedAtMs = System.currentTimeMillis();
        Timestamp startedAt = toTimestamp(startedAtMs);
        Struct resolvedModuleConfig = moduleConfig == null ? Struct.getDefaultInstance() : moduleConfig;
        ServiceContext resolvedContext = context == null ? ServiceContext.getDefaultInstance() : context;

        return executeModuleCall(
                    moduleName,
                    inputDocument,
                    resolvedModuleConfig,
                    resolvedContext
            )
            .map(processResponse -> buildDirectRunResponse(
                    moduleName,
                    resolvedContext,
                    accountId,
                    includeOutputDoc,
                    processResponse,
                    startedAtMs,
                    startedAt
            ))
            .onFailure().recoverWithItem(throwable ->
                    buildRunResponseFromFailure(resolveText(moduleName, "unknown"), throwable, startedAtMs, startedAt)
            );
    }

    public Uni<ModuleTestingSidecarModels.ChainRunResponse> runChainTest(
            PipeDoc inputDocument,
            List<ModuleTestingSidecarModels.ChainStepInput> steps,
            boolean includeFullOutput,
            String accountId
    ) {
        if (inputDocument == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input document is required"));
        }
        if (steps == null || steps.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("at least one chain step is required"));
        }
        if (steps.size() > MAX_CHAIN_STEPS) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                    "too many steps, max supported is " + MAX_CHAIN_STEPS
            ));
        }

        long startedAtMs = System.currentTimeMillis();
        final String streamId = "chain-test-" + UUID.randomUUID();
        List<ModuleTestingSidecarModels.ChainStepInput> orderedSteps = List.copyOf(steps);

        ChainRunState chainState = new ChainRunState(streamId, inputDocument, includeFullOutput);

        Uni<ChainRunState> chain = Uni.createFrom().item(chainState);
        for (int i = 0; i < orderedSteps.size(); i++) {
            final int stepIndex = i;
            final ModuleTestingSidecarModels.ChainStepInput step = orderedSteps.get(i);
            chain = chain.chain(state -> executeChainStep(state, stepIndex, step, streamId));
        }

        return chain
                .map(finalState -> buildChainRunResponse(
                        finalState,
                        true,
                        -1,
                        startedAtMs,
                        orderedSteps.size()
                ))
                .onFailure(ChainStepExecutionException.class)
                .recoverWithItem(ex -> {
                    ChainStepExecutionException failure = (ChainStepExecutionException) ex;
                    return buildChainRunResponse(
                            failure.state(),
                            false,
                            failure.failedStepIndex(),
                            startedAtMs,
                            orderedSteps.size()
                    );
                })
                .onFailure().recoverWithItem(failure -> {
                        LOG.errorf(failure, "Chain test failed with unexpected error");
                        String errorDetail = failure.getMessage();
                        if (failure.getCause() != null) {
                            errorDetail += " — caused by: " + failure.getCause().getMessage();
                        }
                        return new ModuleTestingSidecarModels.ChainRunResponse(
                                false,
                                -1,
                                Math.max(1L, System.currentTimeMillis() - startedAtMs),
                                resolveText(errorDetail, "chain test failed"),
                                List.of()
                        );
                })
                .ifNoItem().after(CHAIN_TEST_TIMEOUT).failWith(new IllegalStateException(
                        "chain test exceeded timeout of " + CHAIN_TEST_TIMEOUT.toMinutes() + " minutes"));
    }

    public Uni<Boolean> isParserModule(String moduleName) {
        if (moduleName == null || moduleName.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("moduleName is required"));
        }

        return loadServiceRegistration(moduleName).map(this::isParserModule);
    }

    public Uni<List<ModuleTestingSidecarModels.ModuleTargetInfo>> listTargets(boolean parserOnly) {
        return listModules()
            .onItem().transformToMulti(modules -> Multi.createFrom().iterable(modules))
            .onItem().transformToUniAndMerge(this::resolveModuleTarget)
            .collect().asList()
            .map(targets -> {
                if (!parserOnly) {
                    return targets;
                }
                return targets.stream()
                    .filter(ModuleTestingSidecarModels.ModuleTargetInfo::parser)
                    .collect(Collectors.toList());
            });
    }

    public Uni<ModuleTestingSidecarModels.RepositoryDocumentsPage> listRepositoryDocuments(
            String drive,
            String connectorId,
            int limit,
            String continuationToken
    ) {
        int resolvedLimit = (limit <= 0 || limit > 200) ? 50 : limit;

        return grpcClientFactory.getClient(repositoryServiceName, MutinyPipeDocServiceGrpc::newMutinyStub)
            .flatMap(stub -> {
                ListPipeDocsRequest.Builder requestBuilder = ListPipeDocsRequest.newBuilder()
                    .setLimit(resolvedLimit);

                if (drive != null && !drive.isBlank()) {
                    requestBuilder.setDrive(drive);
                }

                if (connectorId != null && !connectorId.isBlank()) {
                    requestBuilder.setConnectorId(connectorId);
                }

                if (continuationToken != null && !continuationToken.isBlank()) {
                    requestBuilder.setContinuationToken(continuationToken);
                }

                return stub.listPipeDocs(requestBuilder.build());
            })
            .map(this::toRepositoryDocumentsPage)
            .onFailure().recoverWithItem(failure ->
                new ModuleTestingSidecarModels.RepositoryDocumentsPage(List.of(), "", 0));
    }

    public Uni<PipeDoc> loadDocumentForChain(RepositoryInput repositoryInput) {
        return loadDocumentFromRepository(repositoryInput);
    }

    public Uni<List<GetModuleResponse>> listModules() {
        return grpcClientFactory.getClient(platformRegistrationServiceName, MutinyPlatformRegistrationServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.listPlatformModules(ListPlatformModulesRequest.getDefaultInstance()))
            .map(ListPlatformModulesResponse::getModulesList);
    }

    private Uni<GetServiceRegistrationResponse> loadServiceRegistration(String moduleName) {
        return grpcClientFactory.getClient(moduleName, MutinyPipeStepProcessorServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.getServiceRegistration(GetServiceRegistrationRequest.getDefaultInstance()));
    }

    private Uni<RunModuleTestRequest> validateRequest(RunModuleTestRequest request) {
        if (request == null || request.getModuleName() == null || request.getModuleName().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("module_name is required"));
        }

        if (request.getInputCase() == RunModuleTestRequest.InputCase.INPUT_NOT_SET) {
            return Uni.createFrom().failure(new IllegalArgumentException("input source is required"));
        }

        return Uni.createFrom().item(request);
    }

    private Uni<Void> validateUploadCapability(RunModuleTestRequest request) {
        if (request.getInputCase() != RunModuleTestRequest.InputCase.UPLOAD) {
            return Uni.createFrom().voidItem();
        }

        return loadServiceRegistration(request.getModuleName())
            .onItem().invoke(registration -> {
                if (!isParserModule(registration)) {
                    throw new IllegalArgumentException("Upload input is only supported for parser modules");
                }
            })
            .replaceWithVoid();
    }

    private Uni<PipeDoc> resolveInputDocument(RunModuleTestRequest request) {
        if (request.getInputCase() == RunModuleTestRequest.InputCase.UPLOAD) {
            return buildDocumentFromUpload(request.getUpload());
        }

        if (request.getInputCase() == RunModuleTestRequest.InputCase.REPOSITORY) {
            return loadDocumentFromRepository(request.getRepository());
        }

        return Uni.createFrom().failure(new IllegalArgumentException("unsupported input source"));
    }

    private Uni<PipeDoc> buildDocumentFromUpload(UploadInput upload) {
        if (upload == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("upload payload is required"));
        }
        if (upload.getFileData().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("upload file is empty"));
        }

        return Uni.createFrom().item(() -> {
            SearchMetadata searchMetadata = SearchMetadata.newBuilder()
                .setTitle(upload.getFilename().isBlank() ? "test-upload" : upload.getFilename())
                .setSourceMimeType(upload.getMimeType())
                .build();

            Blob blob = Blob.newBuilder()
                .setBlobId(UUID.randomUUID().toString())
                .setData(upload.getFileData())
                .setMimeType(upload.getMimeType())
                .setFilename(upload.getFilename())
                .setSizeBytes(upload.getFileData().size())
                .build();

            return PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(searchMetadata)
                .setBlobBag(BlobBag.newBuilder().setBlob(blob))
                .build();
        });
    }

    private Uni<PipeDoc> loadDocumentFromRepository(RepositoryInput repositoryInput) {
        String repositoryNodeId = repositoryInput.getRepositoryNodeId();
        if (repositoryNodeId == null || repositoryNodeId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("repository_node_id is required"));
        }

        return grpcClientFactory.getClient(repositoryServiceName, MutinyPipeDocServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.getPipeDoc(GetPipeDocRequest.newBuilder()
                .setNodeId(repositoryNodeId)
                .build()))
            .map(GetPipeDocResponse::getPipedoc)
            .flatMap(doc -> hydrateBlobFromStorageIfNeeded(doc, repositoryInput.getHydrateBlobFromStorage()));
    }

    private Uni<PipeDoc> hydrateBlobFromStorageIfNeeded(PipeDoc document, boolean hydrate) {
        if (document == null || !document.hasBlobBag() || !hydrate) {
            return Uni.createFrom().item(document);
        }

        BlobBag blobBag = document.getBlobBag();
        if (blobBag.getBlobDataCase() != BlobBag.BlobDataCase.BLOB) {
            return Uni.createFrom().item(document);
        }

        Blob blob = blobBag.getBlob();
        if (blob.getContentCase() != Blob.ContentCase.STORAGE_REF) {
            return Uni.createFrom().item(document);
        }

        return grpcClientFactory.getClient(repositoryServiceName, MutinyPipeDocServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(blob.getStorageRef())
                .build()))
            .map((GetBlobResponse response) -> {
                Blob hydratedBlob = blob.toBuilder()
                    .clearContent()
                    .setData(response.getData())
                    .setSizeBytes(response.getSizeBytes())
                    .build();

                if (response.hasMimeType()) {
                    hydratedBlob = hydratedBlob.toBuilder()
                        .setMimeType(response.getMimeType())
                        .build();
                }

                return document.toBuilder()
                    .setBlobBag(BlobBag.newBuilder().setBlob(hydratedBlob))
                    .build();
            });
    }

    private Uni<ProcessDataResponse> executeModuleCall(RunModuleTestRequest request, PipeDoc document) {
        return executeModuleCall(
                request.getModuleName(),
                document,
                request.getModuleConfig(),
                request.hasContext() ? request.getContext() : ServiceContext.getDefaultInstance()
        );
    }

    private Uni<ProcessDataResponse> executeModuleCall(
            String moduleName,
            PipeDoc document,
            Struct moduleConfig,
            ServiceContext context
    ) {
        ProcessDataRequest processDataRequest = ProcessDataRequest.newBuilder()
            .setDocument(document)
            .setMetadata(buildServiceMetadata(context))
            .setConfig(ProcessConfiguration.newBuilder().setJsonConfig(moduleConfig == null ? Struct.getDefaultInstance() : moduleConfig))
            .setIsTest(true)
            .build();

        return grpcClientFactory.getClient(moduleName, MutinyPipeStepProcessorServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.processData(processDataRequest));
    }

    private Uni<ChainRunState> executeChainStep(
            ChainRunState state,
            int stepIndex,
            ModuleTestingSidecarModels.ChainStepInput step,
            String streamId
    ) {
        if (step == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("chain step is required"));
        }
        if (step.moduleName() == null || step.moduleName().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("chain step module_name is required"));
        }

        String moduleName = step.moduleName();
        long stepStartedMs = System.currentTimeMillis();
        GraphNode nodeOverride;
        try {
            nodeOverride = buildGraphNodeOverride(stepIndex, step);
        } catch (Throwable throwable) {
            return Uni.createFrom().failure(new ChainStepExecutionException(state, stepIndex, throwable.getMessage(), throwable));
        }

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(buildChainStream(state.currentDoc, state.currentDoc.getSearchMetadata().getTitle(), streamId, stepIndex))
                .setNodeOverride(nodeOverride)
                .build();

        return grpcClientFactory.getClient(engineServiceName, MutinyEngineV1ServiceGrpc::newMutinyStub)
                .flatMap(stub -> stub.processNode(request))
                .map(response -> {
                    long stepDurationMs = Math.max(1L, System.currentTimeMillis() - stepStartedMs);
                    List<String> engineLogs = extractEngineLogs(response);
                    PipeDoc outputDocument = response.hasUpdatedStream() && response.getUpdatedStream().hasDocument()
                            ? response.getUpdatedStream().getDocument()
                            : state.currentDoc;
                    // Extract module audit trail from the ProcessDataResponse embedded in the stream
                    // The engine stores the module's processor_logs in StepExecutionRecord, but we
                    // also want to check if the output doc itself carries semantic result info
                    List<String> processorLogs = extractModuleProcessorLogs(response);

                    state.results.add(buildChainStepResult(
                            stepIndex,
                            moduleName,
                            response.getSuccess(),
                            stepDurationMs,
                            processorLogs,
                            engineLogs,
                            outputDocument,
                            state.includeFullOutput
                    ));
                    if (!response.getSuccess()) {
                        throw new ChainStepExecutionException(
                                state,
                                stepIndex,
                                resolveText(response.getMessage(), "chain step execution failed")
                        );
                    }
                    state.currentDoc = outputDocument;
                    return state;
                })
                .onFailure().transform(throwable -> {
                    if (throwable instanceof ChainStepExecutionException) {
                        return throwable;
                    }
                    return new ChainStepExecutionException(state, stepIndex, throwable.getMessage(), throwable);
                });
    }

    private GraphNode buildGraphNodeOverride(int stepIndex, ModuleTestingSidecarModels.ChainStepInput step) {
        String nodeId = "test-" + step.moduleName() + "-" + stepIndex;
        GraphNode.Builder builder = GraphNode.newBuilder()
                .setNodeId(nodeId)
                .setModuleId(step.moduleName())
                .setCustomConfig(buildProcessConfig(step.moduleConfig()));

        if (step.preMappings() != null) {
            builder.addAllPreMappings(parseMappings(step.preMappings()));
        }
        if (step.postMappings() != null) {
            builder.addAllPostMappings(parseMappings(step.postMappings()));
        }
        if (step.filterConditions() != null) {
            builder.addAllFilterConditions(step.filterConditions());
        }
        return builder.build();
    }

    private ProcessConfiguration buildProcessConfig(Map<String, Object> moduleConfig) {
        try {
            Struct resolvedModuleConfig = parseStructFromObject(moduleConfig);
            return ProcessConfiguration.newBuilder().setJsonConfig(resolvedModuleConfig).build();
        } catch (RuntimeException runtimeException) {
            throw new IllegalArgumentException("Invalid module_config for chain step: " + runtimeException.getMessage(), runtimeException);
        }
    }

    private PipeStream buildChainStream(PipeDoc document, String streamTitle, String streamId, int stepIndex) {
        String stepNodeId = "test-" + resolveText(streamTitle, "doc") + "-" + stepIndex;
        StreamMetadata metadata = StreamMetadata.newBuilder()
                .setIsTest(true)
                .setDryRun(true)
                .putAllContextParams(Map.of("chainStep", String.valueOf(stepIndex)))
                .setAccountId("unknown")
                .build();

        return PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(document)
                .setCurrentNodeId(stepNodeId)
                .setMetadata(metadata.toBuilder().setIsTest(true).setDryRun(true).build())
                .build();
    }

    private List<String> extractEngineLogs(ProcessNodeResponse response) {
        if (response == null || !response.hasUpdatedStream()) {
            return List.of();
        }
        return response.getUpdatedStream().getMetadata().getHistoryList().stream()
                .flatMap(record -> record.getProcessorLogsList().stream())
                .toList();
    }

    /**
     * Extract module-level processor logs from the engine response.
     * <p>
     * The engine's ProcessNodeResponse carries the full StepExecutionRecord history
     * which includes both engine logs and module processor_logs. For now, module
     * logs are included in the engine history. Once the engine preserves them
     * separately, this method will extract them independently.
     */
    private List<String> extractModuleProcessorLogs(ProcessNodeResponse response) {
        // TODO: Engine currently doesn't separate module processor_logs from engine logs
        // in StepExecutionRecord. When it does, extract them here. For now, the engine
        // logs contain both — the module audit trail is visible there.
        return List.of();
    }

    private ModuleTestingSidecarModels.ChainStepResult buildChainStepResult(
            int stepIndex,
            String moduleName,
            boolean success,
            long durationMs,
            List<String> processorLogs,
            List<String> engineLogs,
            PipeDoc outputDocument,
            boolean includeFullOutput
    ) {
        Map<String, Object> outputSummary = buildOutputDocSummary(outputDocument);
        Map<String, Object> outputDoc = includeFullOutput
                ? toMap(outputDocument)
                : null;

        return new ModuleTestingSidecarModels.ChainStepResult(
                stepIndex,
                moduleName,
                success,
                durationMs,
                processorLogs,
                engineLogs,
                outputSummary,
                outputDoc
        );
    }

    private ModuleTestingSidecarModels.ChainRunResponse buildChainRunResponse(
            ChainRunState state,
            boolean success,
            int failedAtStep,
            long startedAtMs,
            int totalSteps
    ) {
        long totalDurationMs = Math.max(1L, System.currentTimeMillis() - startedAtMs);
        boolean chainSuccess = success && state.results.size() == totalSteps && failedAtStep < 0;
        String message = chainSuccess ? "Chain execution completed"
                : (failedAtStep >= 0 ? "Chain step " + failedAtStep + " failed" : "Chain execution failed");

        return new ModuleTestingSidecarModels.ChainRunResponse(
                chainSuccess,
                failedAtStep,
                totalDurationMs,
                message,
                state.results
        );
    }

    private Map<String, Object> buildOutputDocSummary(PipeDoc outputDocument) {
        Map<String, Object> summary = new HashMap<>();
        if (outputDocument == null) {
            return summary;
        }

        SearchMetadata metadata = outputDocument.getSearchMetadata();
        summary.put("docId", resolveText(outputDocument.getDocId(), ""));
        summary.put("hasBody", metadata.hasBody());
        summary.put("bodyLength", metadata.hasBody() ? metadata.getBody().length() : 0);
        summary.put("hasTitle", metadata.hasTitle());
        if (metadata.hasTitle()) {
            summary.put("title", metadata.getTitle());
        }
        summary.put("semanticResultsCount", metadata.getSemanticResultsCount());
        int totalChunks = metadata.getSemanticResultsList()
                .stream()
                .mapToInt(SemanticProcessingResult::getChunksCount)
                .sum();
        summary.put("totalChunks", totalChunks);
        return summary;
    }

    private List<ProcessingMapping> parseMappings(List<Map<String, Object>> mappings) {
        return mappings.stream()
                .map(this::parseSingleMapping)
                .collect(Collectors.toList());
    }

    private ProcessingMapping parseSingleMapping(Map<String, Object> mappingObj) {
        try {
            String mappingJson = objectMapper.writeValueAsString(mappingObj);
            ProcessingMapping.Builder builder = ProcessingMapping.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(mappingJson, builder);
            return builder.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid processing mapping JSON: " + e.getMessage(), e);
        }
    }

    private Struct parseStructFromObject(Map<String, Object> value) {
        try {
            if (value == null || value.isEmpty()) {
                return Struct.getDefaultInstance();
            }

            String json = objectMapper.writeValueAsString(value);
            Struct.Builder structBuilder = Struct.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(json, structBuilder);
            return structBuilder.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON config: " + e.getMessage(), e);
        }
    }

    private Map<String, Object> toMap(PipeDoc outputDocument) {
        if (outputDocument == null) {
            return null;
        }
        try {
            String json = JsonFormat.printer().omittingInsignificantWhitespace().print(outputDocument);
            return objectMapper.readValue(json, Map.class);
        } catch (InvalidProtocolBufferException e) {
            return Map.of("error", "Unable to serialize output_doc: " + e.getMessage());
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to parse output_doc JSON: " + e.getMessage(), e);
        }
    }

    private static final class ChainRunState {
        private final String streamId;
        private PipeDoc currentDoc;
        private final List<ModuleTestingSidecarModels.ChainStepResult> results;
        private final boolean includeFullOutput;

        private ChainRunState(String streamId, PipeDoc currentDoc, boolean includeFullOutput) {
            this.streamId = streamId;
            this.currentDoc = currentDoc;
            this.includeFullOutput = includeFullOutput;
            this.results = new ArrayList<>();
        }
    }

    private static final class ChainStepExecutionException extends RuntimeException {
        private final ChainRunState state;
        private final int failedStepIndex;

        private ChainStepExecutionException(ChainRunState state, int failedStepIndex, String message) {
            super(message);
            this.state = state;
            this.failedStepIndex = failedStepIndex;
        }

        private ChainStepExecutionException(ChainRunState state, int failedStepIndex, String message, Throwable cause) {
            super(message, cause);
            this.state = state;
            this.failedStepIndex = failedStepIndex;
        }

        private ChainRunState state() {
            return state;
        }

        private int failedStepIndex() {
            return failedStepIndex;
        }
    }

    private ServiceMetadata buildServiceMetadata(ServiceContext context) {
        ServiceContext resolvedContext = context == null ? ServiceContext.getDefaultInstance() : context;
        return ServiceMetadata.newBuilder()
            .setPipelineName(resolveText(resolvedContext.getPipelineName(), defaultPipelineName))
            .setPipeStepName(resolveText(resolvedContext.getPipeStepName(), defaultStepName))
            .setStreamId(resolveText(resolvedContext.getStreamId(), UUID.randomUUID().toString()))
            .setCurrentHopNumber(resolvedContext.getCurrentHopNumber() <= 0 ? 1 : resolvedContext.getCurrentHopNumber())
            .putAllContextParams(resolvedContext.getContextParamsMap())
            .build();
    }

    private ServiceMetadata buildServiceMetadata(RunModuleTestRequest request) {
        return buildServiceMetadata(request.hasContext() ? request.getContext() : ServiceContext.getDefaultInstance());
    }

    private RunModuleTestResponse buildRunResponse(
            RunModuleTestRequest request,
            ProcessDataResponse processResponse,
            long startedAtMs,
            Timestamp startedAt
    ) {
        long completedAtMs = System.currentTimeMillis();
        long durationMs = Math.max(1L, completedAtMs - startedAtMs);

        RunModuleTestResponse.Builder responseBuilder = RunModuleTestResponse.newBuilder()
            .setSuccess(processResponse.getSuccess())
            .setMessage(processResponse.getSuccess() ? "Module execution completed" : "Module execution failed")
            .setDurationMs(durationMs)
            .setElapsedMs(durationMs)
            .setStartedAt(startedAt)
            .setCompletedAt(toTimestamp(completedAtMs))
            .addAllProcessorLogs(processResponse.getProcessorLogsList())
            .setInputSummary(buildInputSummary(request))
            .setOutputSummary(buildOutputSummary(processResponse));

        if (request.getIncludeOutputDoc() && processResponse.hasOutputDoc()) {
            responseBuilder.setOutputDoc(processResponse.getOutputDoc());
        }
        if (!processResponse.getSuccess()) {
            responseBuilder.setErrorCode("PROCESS_DATA_ERROR");
        }

        responseBuilder.setProcessDataResponse(processResponse);

        if (processResponse.hasErrorDetails()) {
            responseBuilder.addErrors(processResponse.getErrorDetails().toString());
        }

        return responseBuilder.build();
    }

    private RunModuleTestResponse buildDirectRunResponse(
            String moduleName,
            ServiceContext context,
            String accountId,
            boolean includeOutputDoc,
            ProcessDataResponse processResponse,
            long startedAtMs,
            Timestamp startedAt
    ) {
        long completedAtMs = System.currentTimeMillis();
        long durationMs = Math.max(1L, completedAtMs - startedAtMs);

        RunModuleTestResponse.Builder responseBuilder = RunModuleTestResponse.newBuilder()
                .setSuccess(processResponse.getSuccess())
                .setMessage(processResponse.getSuccess() ? "Module execution completed" : "Module execution failed")
                .setDurationMs(durationMs)
                .setElapsedMs(durationMs)
                .setStartedAt(startedAt)
                .setCompletedAt(toTimestamp(completedAtMs))
                .addAllProcessorLogs(processResponse.getProcessorLogsList())
                .setInputSummary(buildDirectInputSummary(moduleName, context, accountId, includeOutputDoc))
                .setOutputSummary(buildOutputSummary(processResponse));

        if (includeOutputDoc && processResponse.hasOutputDoc()) {
            responseBuilder.setOutputDoc(processResponse.getOutputDoc());
        }
        if (!processResponse.getSuccess()) {
            responseBuilder.setErrorCode("PROCESS_DATA_ERROR");
        }

        responseBuilder.setProcessDataResponse(processResponse);

        if (processResponse.hasErrorDetails()) {
            responseBuilder.addErrors(processResponse.getErrorDetails().toString());
        }

        return responseBuilder.build();
    }

    private RunModuleTestResponse buildRunResponseFromFailure(
            String moduleName,
            Throwable throwable,
            long startedAtMs,
            Timestamp startedAt
    ) {
        LOG.errorf(throwable, "Failed module test for module=%s", moduleName);

        long completedAtMs = System.currentTimeMillis();
        long durationMs = Math.max(1L, completedAtMs - startedAtMs);

        return RunModuleTestResponse.newBuilder()
            .setSuccess(false)
            .setMessage(resolveText(throwable.getMessage(), "module execution failed"))
            .setDurationMs(durationMs)
            .setElapsedMs(durationMs)
            .setErrorCode(throwable.getClass().getSimpleName())
            .addErrors(resolveText(throwable.getMessage(), "error while executing module"))
            .setStartedAt(startedAt)
            .setCompletedAt(toTimestamp(completedAtMs))
            .build();
    }

    private Uni<ModuleTestingSidecarModels.ModuleTargetInfo> resolveModuleTarget(GetModuleResponse module) {
        return loadServiceRegistration(module.getModuleName())
            .map(registration -> toModuleTargetInfo(module, registration))
            .onFailure().recoverWithItem(failure -> toUnavailableModuleTarget(module, failure.getMessage()));
    }

    private ModuleTestingSidecarModels.ModuleTargetInfo toUnavailableModuleTarget(
            GetModuleResponse module,
            String registrationError
    ) {
        return new ModuleTestingSidecarModels.ModuleTargetInfo(
            module.getModuleName(),
            module.getServiceId(),
            module.getVersion(),
            module.getHost(),
            module.getPort(),
            module.getInputFormat(),
            module.getOutputFormat(),
            module.getDocumentTypesList(),
            List.of(),
            false,
            false,
            "",
            "",
            "",
            module.getIsHealthy(),
            registrationError == null ? "" : registrationError
        );
    }

    private ModuleTestingSidecarModels.ModuleTargetInfo toModuleTargetInfo(
            GetModuleResponse module,
            GetServiceRegistrationResponse registration
    ) {
        List<String> capabilities = registration.hasCapabilities()
            ? registration.getCapabilities().getTypesList().stream().map(Enum::name).toList()
            : List.of();

        boolean parser = registration.hasCapabilities() && registration.getCapabilities().getTypesList().stream()
            .anyMatch(CapabilityType.CAPABILITY_TYPE_PARSER::equals);
        boolean sink = registration.hasCapabilities() && registration.getCapabilities().getTypesList().stream()
            .anyMatch(CapabilityType.CAPABILITY_TYPE_SINK::equals);

        return new ModuleTestingSidecarModels.ModuleTargetInfo(
            module.getModuleName(),
            module.getServiceId(),
            module.getVersion(),
            module.getHost(),
            module.getPort(),
            module.getInputFormat(),
            module.getOutputFormat(),
            module.getDocumentTypesList(),
            capabilities,
            parser,
            sink,
            registration.getJsonConfigSchema(),
            registration.getDisplayName(),
            registration.getDescription(),
            module.getIsHealthy(),
            ""
        );
    }

    private ModuleTestingSidecarModels.RepositoryDocumentsPage toRepositoryDocumentsPage(ListPipeDocsResponse response) {
        List<ModuleTestingSidecarModels.RepositoryDocumentInfo> documents = response.getPipedocsList()
            .stream()
            .map(this::toRepositoryDocumentInfo)
            .toList();

        return new ModuleTestingSidecarModels.RepositoryDocumentsPage(
            documents,
            response.getNextContinuationToken(),
            response.getTotalCount()
        );
    }

    private ModuleTestingSidecarModels.RepositoryDocumentInfo toRepositoryDocumentInfo(PipeDocMetadata documentMetadata) {
        return new ModuleTestingSidecarModels.RepositoryDocumentInfo(
            documentMetadata.getNodeId(),
            documentMetadata.getDocId(),
            documentMetadata.getTitle(),
            documentMetadata.getDocumentType(),
            documentMetadata.getSizeBytes(),
            documentMetadata.getDrive(),
            documentMetadata.getConnectorId(),
            documentMetadata.getCreatedAtEpochMs()
        );
    }

    private Struct buildInputSummary(RunModuleTestRequest request) {
        Struct.Builder summary = Struct.newBuilder()
            .putFields("moduleName", Value.newBuilder().setStringValue(request.getModuleName()).build())
            .putFields("includeOutputDoc", Value.newBuilder().setBoolValue(request.getIncludeOutputDoc()).build())
            .putFields("accountId", Value.newBuilder().setStringValue(resolveText(request.getAccountId(), "unknown")).build())
            .putFields("inputSource", Value.newBuilder().setStringValue(request.getInputCase().name()).build());

        ServiceContext context = request.hasContext() ? request.getContext() : ServiceContext.getDefaultInstance();
        summary.putFields("pipelineName", Value.newBuilder().setStringValue(context.getPipelineName()).build());
        summary.putFields("pipeStepName", Value.newBuilder().setStringValue(context.getPipeStepName()).build());
        summary.putFields("streamId", Value.newBuilder().setStringValue(context.getStreamId()).build());
        summary.putFields("currentHopNumber", Value.newBuilder().setNumberValue(context.getCurrentHopNumber()).build());

        if (request.getInputCase() == RunModuleTestRequest.InputCase.UPLOAD) {
            summary.putFields("uploadFileName", Value.newBuilder().setStringValue(request.getUpload().getFilename()).build());
            summary.putFields("uploadMimeType", Value.newBuilder().setStringValue(request.getUpload().getMimeType()).build());
            summary.putFields("uploadBytes", Value.newBuilder().setNumberValue(request.getUpload().getFileData().size()).build());
        } else if (request.getInputCase() == RunModuleTestRequest.InputCase.REPOSITORY) {
            summary.putFields("repositoryNodeId",
                Value.newBuilder().setStringValue(request.getRepository().getRepositoryNodeId()).build());
            summary.putFields("repositoryDrive",
                Value.newBuilder().setStringValue(request.getRepository().getDrive()).build());
            summary.putFields("repositoryHydration",
                Value.newBuilder().setBoolValue(request.getRepository().getHydrateBlobFromStorage()).build());
        }

        return summary.build();
    }

    private Struct buildDirectInputSummary(
            String moduleName,
            ServiceContext context,
            String accountId,
            boolean includeOutputDoc
    ) {
        ServiceContext resolvedContext = context == null ? ServiceContext.getDefaultInstance() : context;

        return Struct.newBuilder()
                .putFields("moduleName", Value.newBuilder().setStringValue(resolveText(moduleName, "unknown")).build())
                .putFields("inputSource", Value.newBuilder().setStringValue("DIRECT_DOC").build())
                .putFields("includeOutputDoc", Value.newBuilder().setBoolValue(includeOutputDoc).build())
                .putFields("accountId", Value.newBuilder().setStringValue(resolveText(accountId, "unknown")).build())
                .putFields("pipelineName", Value.newBuilder().setStringValue(resolvedContext.getPipelineName()).build())
                .putFields("pipeStepName", Value.newBuilder().setStringValue(resolvedContext.getPipeStepName()).build())
                .putFields("streamId", Value.newBuilder().setStringValue(resolvedContext.getStreamId()).build())
                .putFields("currentHopNumber", Value.newBuilder().setNumberValue(resolvedContext.getCurrentHopNumber()).build())
                .putFields("directDocType", Value.newBuilder().setStringValue("sample_pipe_doc").build())
                .build();
    }

    private Struct buildOutputSummary(ProcessDataResponse processResponse) {
        Struct.Builder summary = Struct.newBuilder()
            .putFields("success", Value.newBuilder().setBoolValue(processResponse.getSuccess()).build())
            .putFields("processorLogCount",
                Value.newBuilder().setNumberValue(processResponse.getProcessorLogsCount()).build());

        if (processResponse.hasOutputDoc()) {
            summary.putFields("outputDocId",
                Value.newBuilder().setStringValue(processResponse.getOutputDoc().getDocId()).build());
            summary.putFields("outputTitle", Value.newBuilder()
                .setStringValue(processResponse.getOutputDoc().getSearchMetadata().getTitle())
                .build());
        }

        if (processResponse.hasErrorDetails()) {
            summary.putFields("errorDetails", Value.newBuilder().setStructValue(processResponse.getErrorDetails()).build());
        }

        return summary.build();
    }

    private boolean isParserModule(GetServiceRegistrationResponse registration) {
        if (!registration.hasCapabilities()) {
            return false;
        }

        return registration.getCapabilities().getTypesList().stream().anyMatch(PARSER_CAPABILITIES::contains);
    }

    private String resolveText(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value;
    }

    private Timestamp toTimestamp(long epochMs) {
        Instant instant = Instant.ofEpochMilli(epochMs);
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }
}
