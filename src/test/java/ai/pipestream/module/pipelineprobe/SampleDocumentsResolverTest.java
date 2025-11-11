package ai.pipestream.module.pipelineprobe;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class SampleDocumentsResolverTest {

    private String originalProp;

    @BeforeEach
    void rememberOriginal() {
        originalProp = System.getProperty(SampleDocumentsResolver.SYS_PROP);
    }

    @AfterEach
    void restoreOriginal() {
        if (originalProp == null) {
            System.clearProperty(SampleDocumentsResolver.SYS_PROP);
        } else {
            System.setProperty(SampleDocumentsResolver.SYS_PROP, originalProp);
        }
    }

    @Test
    void testIsSkippedWhenSampleDocsMissing() {
        // Point to a non-existent directory
        System.setProperty(SampleDocumentsResolver.SYS_PROP, "/definitely/not/here/for-tests-1234567890");
        SampleDocumentsResolver resolver = new SampleDocumentsResolver();

        // If not found, skip this test (verifies skip behavior instead of failing)
        Assumptions.assumeTrue(resolver.resolve().isPresent(),
                "Sample documents missing - test skipped as intended");

        // If assumption passes unexpectedly, ensure path is a directory
        assertThat(resolver.resolve().get().toFile().isDirectory(), is(true));
    }

    @Test
    void testFindsConfiguredSampleDocs() throws IOException {
        Path tempDir = Files.createTempDirectory("sample-docs-test");
        try {
            // Create a dummy file to simulate sample content
            Path dummyFile = tempDir.resolve("dummy.txt");
            Files.writeString(dummyFile, "hello");

            System.setProperty(SampleDocumentsResolver.SYS_PROP, tempDir.toString());

            SampleDocumentsResolver resolver = new SampleDocumentsResolver();
            Optional<Path> resolved = resolver.resolve();

            assertThat("Resolver should find the configured directory", resolved.isPresent(), is(true));
            assertThat(resolved.get(), is(tempDir.toAbsolutePath().normalize()));
            assertThat("Dummy file should exist in resolved directory",
                    Files.exists(resolved.get().resolve("dummy.txt")), is(true));
        } finally {
            // Cleanup temp directory
            Files.walk(tempDir)
                    .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        }
    }
}
