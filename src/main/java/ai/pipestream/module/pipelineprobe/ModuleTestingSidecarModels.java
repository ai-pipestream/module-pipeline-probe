package ai.pipestream.module.pipelineprobe;

import java.util.List;

/**
 * Shared DTOs for module testing sidecar REST responses.
 */
public final class ModuleTestingSidecarModels {

    private ModuleTestingSidecarModels() {
        // Utility container class.
    }

    public record ModuleTargetInfo(
            String moduleName,
            String serviceId,
            String version,
            String host,
            int port,
            String inputFormat,
            String outputFormat,
            List<String> documentTypes,
            List<String> capabilities,
            boolean parser,
            boolean sink,
            String jsonConfigSchema,
            String displayName,
            String description,
            boolean healthy,
            String registrationError
    ) {
    }

    public record RepositoryDocumentInfo(
            String nodeId,
            String docId,
            String title,
            String documentType,
            long sizeBytes,
            String drive,
            String connectorId,
            long createdAtEpochMs
    ) {
    }

    public record RepositoryDocumentsPage(
            List<RepositoryDocumentInfo> documents,
            String nextContinuationToken,
            int totalCount
    ) {
    }

    public record ModuleRunResult(
            String moduleName,
            boolean success,
            String message,
            long durationMs,
            java.util.List<String> processorLogs,
            String errorCode,
            java.util.Map<String, Object> inputSummary,
            java.util.Map<String, Object> outputSummary,
            java.util.Map<String, Object> outputDoc,
            java.util.List<String> errors
    ) {
    }

    public record ChainStepInput(
            String moduleName,
            java.util.Map<String, Object> moduleConfig,
            java.util.List<java.util.Map<String, Object>> preMappings,
            java.util.List<java.util.Map<String, Object>> postMappings,
            java.util.List<String> filterConditions
    ) {
    }

    public record ChainStepResult(
            int stepIndex,
            String moduleName,
            boolean success,
            long durationMs,
            java.util.List<String> processorLogs,
            java.util.List<String> engineLogs,
            java.util.Map<String, Object> outputDocSummary,
            java.util.Map<String, Object> outputDoc
    ) {
    }

    public record ChainRunResponse(
            boolean success,
            int failedAtStep,
            long totalDurationMs,
            String message,
            java.util.List<ChainStepResult> steps
    ) {
    }

}
