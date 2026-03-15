package ai.pipestream.module.pipelineprobe;

import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.testing.harness.v1.RepositoryInput;
import ai.pipestream.testing.harness.v1.RunModuleTestRequest;
import ai.pipestream.testing.harness.v1.ServiceContext;
import ai.pipestream.testing.harness.v1.UploadInput;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Struct;
import com.google.protobuf.Struct.Builder;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TypeRegistry;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.ByteString;
import io.smallrye.mutiny.Uni;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.jboss.resteasy.reactive.multipart.FileUpload;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST resource for the module-testing-sidecar.
 */
@Path("/test-sidecar/v1")
@Produces(MediaType.APPLICATION_JSON)
public class ModuleTestingSidecarResource {

    private static final Logger LOG = Logger.getLogger(ModuleTestingSidecarResource.class);

    @Inject
    @GrpcService
    ModuleTestingSidecarServiceImpl testingService;

    @Inject
    TypeRegistry typeRegistry;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @GET
    @Path("/targets")
    public Uni<List<ModuleTestingSidecarModels.ModuleTargetInfo>> listTargets(
            @QueryParam("parserOnly") @DefaultValue("false") boolean parserOnly
    ) {
        return testingService.listTargets(parserOnly);
    }

    @GET
    @Path("/repository/documents")
    public Uni<ModuleTestingSidecarModels.RepositoryDocumentsPage> listRepositoryDocuments(
            @QueryParam("drive") String drive,
            @QueryParam("connectorId") String connectorId,
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("continuationToken") String continuationToken
    ) {
        return testingService.listRepositoryDocuments(drive, connectorId, limit, continuationToken);
    }

    @GET
    @Path("/debug/last-error")
    public Map<String, Object> getLastError() {
        var last = LastErrorTracker.get();
        if (last == null) {
            return Map.of("message", "No errors recorded");
        }
        return last.toMap();
    }

    @GET
    @Path("/samples")
    public List<Map<String, Object>> listSamples() {
        return Arrays.stream(SampleDocument.values())
                .map(SampleDocument::toInfo)
                .toList();
    }

    @POST
    @Path("/run/sample")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<Response> runSample(RunSampleRequest request) {
        return testingService.isParserModule(request == null ? null : request.moduleName())
                .chain(isParser -> {
                    if (isParser) {
                        return Uni.createFrom().item(() -> buildRunRequestFromSample(request))
                                .flatMap(testingService::runModuleTest)
                                .map(this::protobufToJsonResponse);
                    }

                    return runSampleAsPipeDoc(request)
                            .map(this::protobufToJsonResponse);
                });
    }

    @POST
    @Path("/run/repository")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<Response> runRepository(RunRepositoryRequest request) {
        return testingService.runModuleTest(buildRunRequestFromRepositoryRequest(request))
                .map(this::protobufToJsonResponse);
    }

    @POST
    @Path("/run/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Uni<Response> runUpload(
            @RestForm("moduleName") String moduleName,
            @RestForm("accountId") String accountId,
            @RestForm("includeOutputDoc") @DefaultValue("false") boolean includeOutputDoc,
            @RestForm("moduleConfigJson") String moduleConfigJson,
            @RestForm("pipelineName") String pipelineName,
            @RestForm("pipeStepName") String pipeStepName,
            @RestForm("streamId") String streamId,
            @RestForm("currentHopNumber") long currentHopNumber,
            @RestForm("contextParamsJson") String contextParamsJson,
            @RestForm("file") FileUpload file
    ) {
        return Uni.createFrom().item(() -> {
                    byte[] fileBytes = readUploadedFile(file);
                    return buildRunRequestFromUpload(
                            moduleName,
                            accountId,
                            includeOutputDoc,
                            parseStruct(moduleConfigJson),
                            pipelineName,
                            pipeStepName,
                            streamId,
                            currentHopNumber,
                            parseContextParams(contextParamsJson),
                            file,
                            fileBytes
                    );
                })
                .flatMap(testingService::runModuleTest)
                .map(this::protobufToJsonResponse);
    }

    @POST
    @Path("/run/chain")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<Response> runChain(RunChainRequest request) {
        return resolveChainInputDocument(request)
                .flatMap(inputDocument -> testingService.runChainTest(
                        inputDocument,
                        request.steps(),
                        request.includeFullOutput(),
                        resolveText(request.accountId(), ""))
                )
                .map(chainRunResponse -> Response.ok(chainRunResponse).build());
    }

    private RunModuleTestRequest buildRunRequestFromRepositoryRequest(RunRepositoryRequest request) {
        if (request == null || request.moduleName() == null || request.moduleName().isBlank()) {
            throw new IllegalArgumentException("moduleName is required");
        }
        if (request.repositoryNodeId() == null || request.repositoryNodeId().isBlank()) {
            throw new IllegalArgumentException("repositoryNodeId is required");
        }

        return RunModuleTestRequest.newBuilder()
                .setModuleName(request.moduleName())
                .setModuleConfig(parseStruct(request.moduleConfig()))
                .setIncludeOutputDoc(request.includeOutputDoc())
                .setAccountId(resolveText(request.accountId(), ""))
                .setContext(buildContext(
                    request.pipelineName(),
                    request.pipeStepName(),
                    request.streamId(),
                    request.currentHopNumber(),
                    request.contextParams()
                ))
                .setRepository(RepositoryInput.newBuilder()
                    .setRepositoryNodeId(request.repositoryNodeId())
                    .setDrive(resolveText(request.drive(), ""))
                    .setHydrateBlobFromStorage(request.hydrateBlobFromStorage())
                    .build())
                .build();
    }

    private RunModuleTestRequest buildRunRequestFromUpload(
            String moduleName,
            String accountId,
            boolean includeOutputDoc,
            Struct moduleConfig,
            String pipelineName,
            String pipeStepName,
            String streamId,
            long currentHopNumber,
            Map<String, String> contextParams,
            FileUpload file,
            byte[] fileBytes
    ) {
        if (moduleName == null || moduleName.isBlank()) {
            throw new IllegalArgumentException("moduleName is required");
        }
        if (file == null || file.size() <= 0) {
            throw new IllegalArgumentException("file is required for upload tests");
        }

        return RunModuleTestRequest.newBuilder()
                .setModuleName(moduleName)
                .setModuleConfig(moduleConfig == null ? Struct.getDefaultInstance() : moduleConfig)
                .setIncludeOutputDoc(includeOutputDoc)
                .setAccountId(resolveText(accountId, ""))
                .setContext(buildContext(
                        pipelineName,
                        pipeStepName,
                        streamId,
                        currentHopNumber,
                        contextParams
                ))
                .setUpload(UploadInput.newBuilder()
                        .setFilename(file.fileName())
                        .setMimeType(resolveText(file.contentType(), "application/octet-stream"))
                        .setFileData(ByteString.copyFrom(fileBytes))
                        .build())
                .build();
    }

    private RunModuleTestRequest buildRunRequestFromSample(RunSampleRequest request) {
        if (request == null || request.moduleName() == null || request.moduleName().isBlank()) {
            throw new IllegalArgumentException("moduleName is required");
        }
        if (request.sampleId() == null || request.sampleId().isBlank()) {
            throw new IllegalArgumentException("sampleId is required");
        }

        SampleDocument sample = resolveSample(request.sampleId());
        byte[] fileBytes = loadSampleBytes(sample);
        return RunModuleTestRequest.newBuilder()
                .setModuleName(request.moduleName())
                .setModuleConfig(parseStruct(request.moduleConfig()))
                .setIncludeOutputDoc(request.includeOutputDoc())
                .setAccountId(resolveText(request.accountId(), ""))
                .setContext(buildContext(
                        request.pipelineName(),
                        request.pipeStepName(),
                        request.streamId(),
                        request.currentHopNumber(),
                        request.contextParams()
                ))
                .setUpload(UploadInput.newBuilder()
                        .setFilename(sample.getFileName())
                        .setMimeType(sample.getMimeType())
                        .setFileData(ByteString.copyFrom(fileBytes))
                        .build())
                .build();
    }

    private Uni<PipeDoc> resolveChainInputDocument(RunChainRequest request) {
        if (request == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("request is required"));
        }
        String inputSource = resolveText(request.inputSource(), "sample").toLowerCase();
        return switch (inputSource) {
            case "sample" -> resolveChainSampleDocument(request);
            case "upload" -> resolveChainUploadDocument(request);
            case "repository" -> resolveChainRepositoryDocument(request);
            default -> Uni.createFrom().failure(new IllegalArgumentException(
                    "unsupported input_source: " + request.inputSource()
            ));
        };
    }

    private Uni<PipeDoc> resolveChainSampleDocument(RunChainRequest request) {
        String sampleId = resolveText(request.sampleId(), request.sampleName());
        if (sampleId == null || sampleId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("sampleId is required when input_source=sample"));
        }
        SampleDocument sample = resolveSample(sampleId);
        byte[] fileBytes = loadSampleBytes(sample);
        return Uni.createFrom().item(() -> buildPipeDocFromSample(sample, fileBytes));
    }

    private Uni<PipeDoc> resolveChainUploadDocument(RunChainRequest request) {
        RunChainUpload upload = request.upload();
        if (upload == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("upload is required when input_source=upload"));
        }
        if (upload.filename() == null || upload.filename().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("upload.filename is required"));
        }
        if (upload.mimeType() == null || upload.mimeType().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("upload.mimeType is required"));
        }
        if (upload.base64Data() == null || upload.base64Data().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("upload.base64Data is required"));
        }
        byte[] fileBytes;
        try {
            fileBytes = Base64.getDecoder().decode(upload.base64Data());
        } catch (IllegalArgumentException e) {
            return Uni.createFrom().failure(new IllegalArgumentException("upload.base64Data must be valid base64", e));
        }
        return Uni.createFrom().item(() -> buildPipeDocFromUploadData(upload.filename(), upload.mimeType(), fileBytes));
    }

    private Uni<PipeDoc> resolveChainRepositoryDocument(RunChainRequest request) {
        if (request.repositoryNodeId() == null || request.repositoryNodeId().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("repository_node_id is required when input_source=repository"));
        }
        return testingService.loadDocumentForChain(
                RepositoryInput.newBuilder()
                        .setRepositoryNodeId(request.repositoryNodeId())
                        .setDrive(resolveText(request.repositoryDrive(), ""))
                        .setHydrateBlobFromStorage(request.repositoryHydrateBlobFromStorage())
                        .build()
        );
    }

    private Uni<ai.pipestream.testing.harness.v1.RunModuleTestResponse> runSampleAsPipeDoc(RunSampleRequest request) {
        if (request == null || request.moduleName() == null || request.moduleName().isBlank()) {
            throw new IllegalArgumentException("moduleName is required");
        }
        if (request.sampleId() == null || request.sampleId().isBlank()) {
            throw new IllegalArgumentException("sampleId is required");
        }

        SampleDocument sample = resolveSample(request.sampleId());
        if (!isTextSample(sample)) {
            throw new IllegalArgumentException("The selected module is non-parser, so sample input must include text content directly. "
                    + "Use text samples or run a parser first to produce a repository fixture.");
        }

        byte[] fileBytes = loadSampleBytes(sample);
        PipeDoc inputDocument = buildPipeDocFromSample(sample, fileBytes);

        return testingService.runModuleTestWithPipeDoc(
                request.moduleName(),
                parseStruct(request.moduleConfig()),
                request.accountId(),
                buildContext(
                        request.pipelineName(),
                        request.pipeStepName(),
                        request.streamId(),
                        request.currentHopNumber(),
                        request.contextParams()
                ),
                inputDocument,
                request.includeOutputDoc()
        );
    }

    private boolean isTextSample(SampleDocument sample) {
        return sample != null
                && sample.getMimeType() != null
                && sample.getMimeType().toLowerCase().startsWith("text/");
    }

    private SampleDocument resolveSample(String sampleId) {
        if (sampleId == null || sampleId.isBlank()) {
            throw new IllegalArgumentException("sampleId is required");
        }
        return SampleDocument.fromId(sampleId);
    }

    private byte[] loadSampleBytes(SampleDocument sample) {
        try {
            return sample.loadBytes();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load sample file: " + sample.getFileName(), e);
        }
    }

    private PipeDoc buildPipeDocFromSample(SampleDocument sample, byte[] fileBytes) {
        return buildPipeDocFromSampleOrBinary(sample.getTitle(), sample.getMimeType(), sample.getFileName(), fileBytes);
    }

    private PipeDoc buildPipeDocFromSampleOrBinary(String title, String mimeType, String fileName, byte[] fileBytes) {
        SearchMetadata.Builder metadataBuilder = SearchMetadata.newBuilder()
                .setTitle(resolveText(title, "test-sample"))
                .setSourceMimeType(resolveText(mimeType, "application/octet-stream"));

        if (isTextMimeType(metadataBuilder.getSourceMimeType())) {
            metadataBuilder.setBody(new String(fileBytes, StandardCharsets.UTF_8));
        }

        Blob blob = Blob.newBuilder()
                .setBlobId(UUID.randomUUID().toString())
                .setMimeType(resolveText(mimeType, "application/octet-stream"))
                .setFilename(fileName == null || fileName.isBlank() ? "sample.bin" : fileName)
                .setSizeBytes(fileBytes.length)
                .setData(ByteString.copyFrom(fileBytes))
                .build();

        return PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(metadataBuilder.build())
                .setBlobBag(BlobBag.newBuilder().setBlob(blob))
                .build();
    }

    private PipeDoc buildPipeDocFromUploadData(String fileName, String mimeType, byte[] fileBytes) {
        return buildPipeDocFromSampleOrBinary(fileName, mimeType, fileName, fileBytes);
    }

    private boolean isTextMimeType(String mimeType) {
        return mimeType != null && mimeType.toLowerCase().startsWith("text/");
    }

    private ServiceContext buildContext(
            String pipelineName,
            String pipeStepName,
            String streamId,
            long currentHopNumber,
            Map<String, String> contextParams
    ) {
        return ServiceContext.newBuilder()
                .setPipelineName(resolveText(pipelineName, "module-testing-sidecar"))
                .setPipeStepName(resolveText(pipeStepName, "module-testing-step"))
                .setStreamId(resolveText(streamId, ""))
                .setCurrentHopNumber(currentHopNumber <= 0 ? 1 : currentHopNumber)
                .putAllContextParams(contextParams == null ? Collections.emptyMap() : contextParams)
                .build();
    }

    private Struct parseStruct(String json) {
        if (json == null || json.isBlank()) {
            return Struct.getDefaultInstance();
        }

        Builder structBuilder = Struct.newBuilder();
        try {
            JsonFormat.parser().merge(json, structBuilder);
            return structBuilder.build();
        } catch (Exception e) {
            LOG.error("Invalid moduleConfig JSON", e);
            throw new IllegalArgumentException("moduleConfig must be valid JSON object");
        }
    }

    private Map<String, String> parseContextParams(String contextParamsJson) {
        if (contextParamsJson == null || contextParamsJson.isBlank()) {
            return Collections.emptyMap();
        }

        try {
            return objectMapper.readValue(contextParamsJson, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            LOG.error("Invalid contextParamsJson", e);
            throw new IllegalArgumentException("contextParamsJson must be valid JSON map");
        }
    }

    private byte[] readUploadedFile(FileUpload file) {
        if (file == null || file.uploadedFile() == null) {
            throw new IllegalArgumentException("No file uploaded");
        }
        try {
            File uploadedFile = file.uploadedFile().toFile();
            return Files.readAllBytes(uploadedFile.toPath());
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read uploaded file", e);
        }
    }

    private String resolveText(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    @ServerExceptionMapper
    public Response mapValidationErrors(IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                .entity(Map.of("error", e.getMessage()))
                .build();
    }

    private Response protobufToJsonResponse(MessageOrBuilder message) {
        try {
            String json = JsonFormat.printer()
                    .usingTypeRegistry(typeRegistry)
                    .includingDefaultValueFields()
                    .print(message);
            return Response.ok(json, MediaType.APPLICATION_JSON).build();
        } catch (InvalidProtocolBufferException e) {
            LOG.warnf("TypeRegistry miss during JSON serialization, retrying without defaults: %s", e.getMessage());
            try {
                String json = JsonFormat.printer()
                        .usingTypeRegistry(typeRegistry)
                        .print(message);
                return Response.ok(json, MediaType.APPLICATION_JSON).build();
            } catch (Exception inner) {
                LOG.warnf("JSON serialization still failed, falling back to TextFormat: %s", inner.getMessage());
                String text = TextFormat.printer().printToString(message);
                String fallback = "{\"_format\":\"protobuf-text\",\"data\":" +
                        objectMapper.valueToTree(text) + "}";
                return Response.ok(fallback, MediaType.APPLICATION_JSON).build();
            }
        } catch (Exception e) {
            LOG.error("Failed to serialize protobuf response", e);
            return Response.serverError()
                    .entity(Map.of("error", "Failed to serialize response: " + e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    public record RunRepositoryRequest(
            String moduleName,
            String repositoryNodeId,
            String drive,
            boolean hydrateBlobFromStorage,
            String moduleConfig,
            boolean includeOutputDoc,
            String accountId,
            String pipelineName,
            String pipeStepName,
            String streamId,
            long currentHopNumber,
            Map<String, String> contextParams
    ) {
    }

    public record RunSampleRequest(
            String sampleId,
            String moduleName,
            String moduleConfig,
            boolean includeOutputDoc,
            String accountId,
            String pipelineName,
            String pipeStepName,
            String streamId,
            long currentHopNumber,
            Map<String, String> contextParams
    ) {
    }

    public record RunChainUpload(
            String filename,
            String mimeType,
            String base64Data
    ) {
    }

    public record RunChainRequest(
            String inputSource,
            String sampleId,
            String sampleName,
            String repositoryNodeId,
            String repositoryDrive,
            boolean repositoryHydrateBlobFromStorage,
            RunChainUpload upload,
            java.util.List<ModuleTestingSidecarModels.ChainStepInput> steps,
            boolean includeFullOutput,
            String accountId
    ) {
    }
}
