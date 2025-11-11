package ai.pipestream.module.pipelineprobe;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Resolves the sample-documents directory location from configuration.
 * Lookup order (first non-empty wins):
 *  - System property: ai.pipestream.sampleDocumentsDir
 *  - Env var: SAMPLE_DOCUMENTS_DIR
 *  - MP/Quarkus config property: ai.pipestream.sample-documents.dir
 *  - Fallback common path relative to repo root: ../sample-documents (best-effort)
 *
 * If the resolved path does not exist on the filesystem, {@link #resolve()} returns Optional.empty().
 */
class SampleDocumentsResolver {

    static final String SYS_PROP = "ai.pipestream.sampleDocumentsDir";
    static final String ENV_VAR = "SAMPLE_DOCUMENTS_DIR";
    static final String CFG_PROP = "ai.pipestream.sample-documents.dir";

    Optional<Path> resolve() {
        // 1) System property
        String sys = System.getProperty(SYS_PROP);
        Optional<Path> fromSys = toExistingPath(sys);
        if (fromSys.isPresent()) return fromSys;

        // 2) Environment variable
        String env = System.getenv(ENV_VAR);
        Optional<Path> fromEnv = toExistingPath(env);
        if (fromEnv.isPresent()) return fromEnv;

        // 3) MicroProfile/Quarkus config
        try {
            Config cfg = ConfigProvider.getConfig();
            String cfgVal = cfg.getOptionalValue(CFG_PROP, String.class).orElse(null);
            Optional<Path> fromCfg = toExistingPath(cfgVal);
            if (fromCfg.isPresent()) return fromCfg;
        } catch (Throwable ignored) {
            // ConfigProvider may not be initialized in plain unit tests; ignore and continue
        }

        // 4) Common fallback relative to module directory
        // Try repo root: projectRoot/../../sample-documents (ai-pipestream/sample-documents)
        Path fallback1 = Paths.get("..", "..", "sample-documents").normalize().toAbsolutePath();
        if (Files.isDirectory(fallback1)) {
            return Optional.of(fallback1);
        }
        // Try sibling of this module: projectRoot/../sample-documents (in-progress/sample-documents)
        Path fallback2 = Paths.get("..", "sample-documents").normalize().toAbsolutePath();
        if (Files.isDirectory(fallback2)) {
            return Optional.of(fallback2);
        }

        return Optional.empty();
    }

    private Optional<Path> toExistingPath(String raw) {
        if (raw == null || raw.isBlank()) return Optional.empty();
        Path p = Paths.get(raw).toAbsolutePath().normalize();
        return Files.isDirectory(p) ? Optional.of(p) : Optional.empty();
    }
}
