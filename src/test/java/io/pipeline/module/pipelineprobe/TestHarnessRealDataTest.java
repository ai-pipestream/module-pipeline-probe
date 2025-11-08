package io.pipeline.module.pipelineprobe;

import com.google.protobuf.Empty;
import io.pipeline.data.v1.PipeDoc;
import io.pipeline.data.v1.PipeStream;
import io.pipeline.data.module.ModuleProcessRequest;
import io.pipeline.data.module.ServiceMetadata;
// import io.pipeline.data.util.proto.ProtobufTestDataHelper;  // Temporarily disabled
import io.pipeline.testing.harness.grpc.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Custom test data helper for module-pipeline-probe tests
 * Provides mock data for testing without external dependencies
 */
class TestDataHelper {

    Collection<PipeDoc> getSamplePipeDocuments() {
        List<PipeDoc> docs = new ArrayList<>();
        // Generate 100 sample documents for bulk processing tests
        for (int i = 1; i <= 100; i++) {
            docs.add(createTestDoc("test-doc-" + i, "Sample Document " + i,
                "This is sample document content " + i + ". This document contains some test data for processing."));
        }
        return docs;
    }

    Collection<PipeDoc> getTikaPipeDocuments() {
        return List.of(
            createTestDoc("tika-doc-1", "TIKA Document 1", "Parsed content from TIKA processing 1"),
            createTestDoc("tika-doc-2", "TIKA Document 2", "Parsed content from TIKA processing 2")
        );
    }

    Collection<PipeDoc> getChunkerPipeDocuments() {
        return List.of(
            createTestDoc("chunk-doc-1", "Chunk Document 1", "This document will be chunked into smaller pieces"),
            createTestDoc("chunk-doc-2", "Chunk Document 2", "Another document for chunking tests")
        );
    }

    Collection<PipeStream> getSamplePipeStreams() {
        return List.of(
            PipeStream.newBuilder()
                .setStreamId("test-stream-1")
                .setDocument(createTestDoc("stream-doc-1", "Stream Document", "Document in a stream"))
                .build(),
            PipeStream.newBuilder()
                .setStreamId("test-stream-2")
                .setDocument(createTestDoc("stream-doc-2", "Another Stream Doc", "Another document in stream"))
                .build()
        );
    }

    private PipeDoc createTestDoc(String id, String title, String body) {
        return PipeDoc.newBuilder()
            .setDocId(id)
            .setSearchMetadata(
                io.pipeline.data.v1.SearchMetadata.newBuilder()
                    .setTitle(title)
                    .setBody(body)
                    .build()
            )
            .build();
    }
}

/**
 * Comprehensive test suite for the TestHarness service using real test data.
 * Tests the TestHarness's ability to process various document types and simulate
 * different pipeline behaviors.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@QuarkusTest
class TestHarnessRealDataTest {

    private static final Logger LOG = Logger.getLogger(TestHarnessRealDataTest.class);

    @GrpcClient
    TestHarness testHarness;


    TestDataHelper testDataHelper = new TestDataHelper();

    @BeforeEach
    void setup() {
        // Reset any simulation settings to ensure tests start with clean state
        TestCommand resetCommand = TestCommand.newBuilder()
                .setCommandId("reset-before-test")
                .setTimestamp(createTimestamp())
                .setSimulateScenario(SimulateScenarioCommand.newBuilder()
                        .setScenario(SimulateScenarioCommand.Scenario.NORMAL_PROCESSING)
                        .build())
                .build();

        testHarness.executeTest(resetCommand).await().atMost(Duration.ofSeconds(5));
    }

    private Stream<PipeDoc> sampleDocuments() {
        return testDataHelper.getSamplePipeDocuments().stream().limit(10);
    }

    private Stream<PipeDoc> tikaDocuments() {
        return testDataHelper.getTikaPipeDocuments().stream().limit(5);
    }

    @Test
    @org.junit.jupiter.api.Disabled("Disabled due to test environment setup")
    void testModuleStatusAfterProcessing() {
        // Given
        LOG.debug("Testing module status changes after document processing");
        
        // Get initial status
        ModuleStatus initialStatus = testHarness.getModuleStatus(Empty.getDefaultInstance())
                .await().atMost(Duration.ofSeconds(5));

        long initialProcessed = initialStatus.getDocumentsProcessed();
        LOG.debugf("Initial documents processed count: %d", initialProcessed);

        // Process a document
        PipeDoc doc = testDataHelper.getSamplePipeDocuments().iterator().next();
        ModuleProcessRequest processRequest = createProcessRequest(doc, "status-test");

        TestCommand command = TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(createTimestamp())
                .setProcessDocument(ProcessDocumentCommand.newBuilder()
                        .setRequest(processRequest)
                        .setExpectSuccess(true)
                        .setTimeoutMs(5000)
                        .build())
                .build();

        // When
        TestResult result = testHarness.executeTest(command)
                .await().atMost(Duration.ofSeconds(10));

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(result.getSuccess()).isTrue() -> Hamcrest: assertThat("Processing should be successful", result.getSuccess(), is(true))
        assertThat("Processing should be successful", result.getSuccess(), is(true));

        // Get updated status
        ModuleStatus updatedStatus = testHarness.getModuleStatus(Empty.getDefaultInstance())
                .await().atMost(Duration.ofSeconds(5));

        // AssertJ: assertThat(updatedStatus.getDocumentsProcessed()).isGreaterThan(initialProcessed) -> Hamcrest: assertThat("Documents processed count should increase", updatedStatus.getDocumentsProcessed(), is(greaterThan(initialProcessed)))
        assertThat("Documents processed count should increase", updatedStatus.getDocumentsProcessed(), is(greaterThan(initialProcessed)));
        
        LOG.debugf("Status test completed - initial: %d, updated: %d, document ID: %s", 
                  initialProcessed, updatedStatus.getDocumentsProcessed(), doc.getDocId());
    }

    @ParameterizedTest
    @MethodSource("sampleDocuments")
    void testProcessSampleDocuments(PipeDoc sampleDoc) {
        ModuleProcessRequest processRequest = createProcessRequest(sampleDoc, "sample-test");

        TestCommand command = TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(createTimestamp())
                .setProcessDocument(ProcessDocumentCommand.newBuilder()
                        .setRequest(processRequest)
                        .setExpectSuccess(true)
                        .setTimeoutMs(5000)
                        .build())
                .build();

        TestResult result = testHarness.executeTest(command)
                .await().atMost(Duration.ofSeconds(10));

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(result.getSuccess()).isTrue() -> Hamcrest: assertThat("Sample document processing should be successful", result.getSuccess(), is(true))
        assertThat("Sample document processing should be successful", result.getSuccess(), is(true));
        
        // AssertJ: assertThat(result.getMessagesCount()).isGreaterThan(0) -> Hamcrest: assertThat("Should have processing messages", result.getMessagesCount(), is(greaterThan(0)))
        assertThat("Should have processing messages", result.getMessagesCount(), is(greaterThan(0)));

        // Verify we got expected events
        boolean hasReceivedEvent = result.getEventsList().stream()
                .anyMatch(TestEvent::hasDocumentReceived);
        boolean hasProcessedEvent = result.getEventsList().stream()
                .anyMatch(TestEvent::hasDocumentProcessed);

        // AssertJ: assertThat(hasReceivedEvent).isTrue() -> Hamcrest: assertThat("Should have document received event", hasReceivedEvent, is(true))
        assertThat("Should have document received event", hasReceivedEvent, is(true));
        
        // AssertJ: assertThat(hasProcessedEvent).isTrue() -> Hamcrest: assertThat("Should have document processed event", hasProcessedEvent, is(true))
        assertThat("Should have document processed event", hasProcessedEvent, is(true));

        // Check the processed event details
        TestEvent processedEvent = result.getEventsList().stream()
                .filter(TestEvent::hasDocumentProcessed)
                .findFirst()
                .orElse(null);

        // AssertJ: assertThat(processedEvent).isNotNull() -> Hamcrest: assertThat("Processed event should be present", processedEvent, is(notNullValue()))
        assertThat("Processed event should be present", processedEvent, is(notNullValue()));
        
        // AssertJ: assertThat(processedEvent.getDocumentProcessed().getSuccess()).isTrue() -> Hamcrest: assertThat("Processed event should indicate success", processedEvent.getDocumentProcessed().getSuccess(), is(true))
        assertThat("Processed event should indicate success", processedEvent.getDocumentProcessed().getSuccess(), is(true));
        
        // AssertJ: assertThat(processedEvent.getDocumentProcessed().getDocumentId()).isEqualTo(sampleDoc.getDocId()) -> Hamcrest: assertThat("Processed document ID should match original", processedEvent.getDocumentProcessed().getDocumentId(), is(sampleDoc.getDocId()))
        assertThat("Processed document ID should match original", processedEvent.getDocumentProcessed().getDocumentId(), is(sampleDoc.getDocId()));
    }

    @ParameterizedTest
    @MethodSource("tikaDocuments")
    void testProcessTikaDocuments(PipeDoc tikaDoc) {
        ModuleProcessRequest processRequest = createProcessRequest(tikaDoc, "tika-test");

        TestCommand command = TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(createTimestamp())
                .setProcessDocument(ProcessDocumentCommand.newBuilder()
                        .setRequest(processRequest)
                        .setExpectSuccess(true)
                        .setTimeoutMs(10000) // Longer timeout for potentially larger docs
                        .build())
                .build();

        TestResult result = testHarness.executeTest(command)
                .await().atMost(Duration.ofSeconds(15));

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(result.getSuccess()).isTrue() -> Hamcrest: assertThat("Tika document processing should be successful", result.getSuccess(), is(true))
        assertThat("Tika document processing should be successful", result.getSuccess(), is(true));

        // Verify processing metadata
        TestEvent processedEvent = result.getEventsList().stream()
                .filter(TestEvent::hasDocumentProcessed)
                .findFirst()
                .orElse(null);

        // AssertJ: assertThat(processedEvent).isNotNull() -> Hamcrest: assertThat("Tika processed event should be present", processedEvent, is(notNullValue()))
        assertThat("Tika processed event should be present", processedEvent, is(notNullValue()));
        
        // AssertJ: assertThat(processedEvent.getDocumentProcessed().getProcessingTimeMs()).isGreaterThan(0) -> Hamcrest: assertThat("Processing time should be greater than 0", processedEvent.getDocumentProcessed().getProcessingTimeMs(), is(greaterThan(0L)))
        assertThat("Processing time should be greater than 0", processedEvent.getDocumentProcessed().getProcessingTimeMs(), is(greaterThan(0L)));
    }

    @Test
    void testStreamProcessing() {
        // Test bidirectional streaming with multiple commands
        List<TestCommand> commands = new ArrayList<>();

        // Add health check command
        commands.add(TestCommand.newBuilder()
                .setCommandId("health-check-1")
                .setTimestamp(createTimestamp())
                .setCheckHealth(CheckHealthCommand.newBuilder()
                        .setIncludeDetails(true)
                        .build())
                .build());

        // Add document processing commands
        Collection<PipeDoc> docs = testDataHelper.getSamplePipeDocuments();
        int count = 0;
        for (PipeDoc doc : docs) {
            if (count++ >= 3) break; // Process first 3 documents

            ModuleProcessRequest processRequest = createProcessRequest(doc, "stream-test");
            commands.add(TestCommand.newBuilder()
                    .setCommandId("process-" + count)
                    .setTimestamp(createTimestamp())
                    .setProcessDocument(ProcessDocumentCommand.newBuilder()
                            .setRequest(processRequest)
                            .setExpectSuccess(true)
                            .setTimeoutMs(5000)
                            .build())
                    .build());
        }

        // Send commands as a stream and collect events
        Multi<TestEvent> eventStream = testHarness.executeTestStream(Multi.createFrom().items(commands.stream()));

        AssertSubscriber<TestEvent> subscriber = eventStream
                .subscribe().withSubscriber(AssertSubscriber.create(20));

        subscriber.awaitCompletion(Duration.ofSeconds(30));

        List<TestEvent> events = subscriber.getItems();
        
        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(events).isNotEmpty() -> Hamcrest: assertThat("Stream should produce events", events, is(not(empty())))
        assertThat("Stream should produce events", events, is(not(empty())));

        // Verify we got events for each command
        long healthEvents = events.stream().filter(TestEvent::hasHealthCheck).count();
        long receivedEvents = events.stream().filter(TestEvent::hasDocumentReceived).count();
        long processedEvents = events.stream().filter(TestEvent::hasDocumentProcessed).count();

        // AssertJ: assertThat(healthEvents).isGreaterThanOrEqualTo(1) -> Hamcrest: assertThat("Should have health check events", healthEvents, is(greaterThanOrEqualTo(1L)))
        assertThat("Should have health check events", healthEvents, is(greaterThanOrEqualTo(1L)));
        
        // AssertJ: assertThat(receivedEvents).isEqualTo(3) -> Hamcrest: assertThat("Should have exactly 3 document received events", receivedEvents, is(3L))
        assertThat("Should have exactly 3 document received events", receivedEvents, is(3L));
        
        // AssertJ: assertThat(processedEvents).isEqualTo(3) -> Hamcrest: assertThat("Should have exactly 3 document processed events", processedEvents, is(3L))
        assertThat("Should have exactly 3 document processed events", processedEvents, is(3L));
    }

    @Test
    void testSimulateSlowProcessing() {
        PipeDoc doc = testDataHelper.getSamplePipeDocuments().iterator().next();
        ModuleProcessRequest processRequest = createProcessRequest(doc, "slow-test");

        // First, configure the module for slow processing
        TestCommand configCommand = TestCommand.newBuilder()
                .setCommandId("config-slow")
                .setTimestamp(createTimestamp())
                .setSimulateScenario(SimulateScenarioCommand.newBuilder()
                        .setScenario(SimulateScenarioCommand.Scenario.SLOW_PROCESSING)
                        .setDurationMs(30000) // 30 seconds
                        .build())
                .build();

        TestResult configResult = testHarness.executeTest(configCommand)
                .await().atMost(Duration.ofSeconds(5));
                
        // AssertJ: assertThat(configResult.getSuccess()).isTrue() -> Hamcrest: assertThat("Configuration should succeed", configResult.getSuccess(), is(true))
        assertThat("Configuration should succeed", configResult.getSuccess(), is(true));

        // Now process a document - it should be slow
        TestCommand processCommand = TestCommand.newBuilder()
                .setCommandId("process-slow")
                .setTimestamp(createTimestamp())
                .setProcessDocument(ProcessDocumentCommand.newBuilder()
                        .setRequest(processRequest)
                        .setExpectSuccess(true)
                        .setTimeoutMs(15000)
                        .build())
                .build();

        long startTime = System.currentTimeMillis();
        TestResult result = testHarness.executeTest(processCommand)
                .await().atMost(Duration.ofSeconds(20));
        long duration = System.currentTimeMillis() - startTime;

        // AssertJ: assertThat(result.getSuccess()).isTrue() -> Hamcrest: assertThat("Slow processing should eventually succeed", result.getSuccess(), is(true))
        assertThat("Slow processing should eventually succeed", result.getSuccess(), is(true));
        
        // AssertJ: assertThat(duration).isGreaterThan(2000) -> Hamcrest: assertThat("Processing should take at least 2 seconds due to simulation", duration, is(greaterThan(2000L)))
        assertThat("Processing should take at least 2 seconds due to simulation", duration, is(greaterThan(2000L)));

        // Reset to normal processing
        TestCommand resetCommand = TestCommand.newBuilder()
                .setCommandId("reset-normal")
                .setTimestamp(createTimestamp())
                .setSimulateScenario(SimulateScenarioCommand.newBuilder()
                        .setScenario(SimulateScenarioCommand.Scenario.NORMAL_PROCESSING)
                        .build())
                .build();

        testHarness.executeTest(resetCommand).await().atMost(Duration.ofSeconds(5));
    }

    @Test
    void testSimulateRandomFailures() {
        // Configure random failures
        TestCommand configCommand = TestCommand.newBuilder()
                .setCommandId("config-failures")
                .setTimestamp(createTimestamp())
                .setSimulateScenario(SimulateScenarioCommand.newBuilder()
                        .setScenario(SimulateScenarioCommand.Scenario.RANDOM_FAILURES)
                        .setDurationMs(60000) // 1 minute
                        .build())
                .build();

        TestResult configResult = testHarness.executeTest(configCommand)
                .await().atMost(Duration.ofSeconds(5));
                
        // AssertJ: assertThat(configResult.getSuccess()).isTrue() -> Hamcrest: assertThat("Configuration should succeed", configResult.getSuccess(), is(true))
        assertThat("Configuration should succeed", configResult.getSuccess(), is(true));

        // Process multiple documents - some should fail
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        Collection<PipeDoc> docs = testDataHelper.getSamplePipeDocuments();
        int processed = 0;
        for (PipeDoc doc : docs) {
            if (processed++ >= 10) break;

            ModuleProcessRequest processRequest = createProcessRequest(doc, "failure-test");
            TestCommand command = TestCommand.newBuilder()
                    .setCommandId("process-" + processed)
                    .setTimestamp(createTimestamp())
                    .setProcessDocument(ProcessDocumentCommand.newBuilder()
                            .setRequest(processRequest)
                            .setExpectSuccess(false) // Don't require success
                            .setTimeoutMs(5000)
                            .build())
                    .build();

            TestResult result = testHarness.executeTest(command)
                    .await().atMost(Duration.ofSeconds(10));

            if (result.getSuccess()) {
                result.getEventsList().stream()
                        .filter(TestEvent::hasDocumentProcessed)
                        .findFirst()
                        .ifPresent(event -> {
                            if (event.getDocumentProcessed().getSuccess()) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        });
            }
        }

        // With random failures, we should have some of each
        // AssertJ: assertThat(successCount.get()).isGreaterThan(0) -> Hamcrest: assertThat("Should have some successful processing", successCount.get(), is(greaterThan(0)))
        assertThat("Should have some successful processing", successCount.get(), is(greaterThan(0)));
        
        // AssertJ: assertThat(failureCount.get()).isGreaterThan(0) -> Hamcrest: assertThat("Should have some failed processing due to simulation", failureCount.get(), is(greaterThan(0)))
        assertThat("Should have some failed processing due to simulation", failureCount.get(), is(greaterThan(0)));

        // Reset to normal
        TestCommand resetCommand = TestCommand.newBuilder()
                .setCommandId("reset-normal")
                .setTimestamp(createTimestamp())
                .setSimulateScenario(SimulateScenarioCommand.newBuilder()
                        .setScenario(SimulateScenarioCommand.Scenario.NORMAL_PROCESSING)
                        .build())
                .build();

        testHarness.executeTest(resetCommand).await().atMost(Duration.ofSeconds(5));
    }

    @Test
    void testBulkProcessingPerformance() {
        // Test processing a larger batch of documents
        Collection<PipeDoc> docs = testDataHelper.getSamplePipeDocuments();
        List<TestCommand> commands = new ArrayList<>();

        int count = 0;
        for (PipeDoc doc : docs) {
            if (count++ >= 50) break; // Process 50 documents

            ModuleProcessRequest processRequest = createProcessRequest(doc, "bulk-test");
            commands.add(TestCommand.newBuilder()
                    .setCommandId("bulk-" + count)
                    .setTimestamp(createTimestamp())
                    .setProcessDocument(ProcessDocumentCommand.newBuilder()
                            .setRequest(processRequest)
                            .setExpectSuccess(true)
                            .setTimeoutMs(5000)
                            .build())
                    .build());
        }

        long startTime = System.currentTimeMillis();

        // Process all commands via streaming
        Multi<TestEvent> eventStream = testHarness.executeTestStream(Multi.createFrom().items(commands.stream()));

        AssertSubscriber<TestEvent> subscriber = eventStream
                .subscribe().withSubscriber(AssertSubscriber.create(200));

        subscriber.awaitCompletion(Duration.ofMinutes(2));

        long duration = System.currentTimeMillis() - startTime;

        List<TestEvent> events = subscriber.getItems();
        long processedCount = events.stream()
                .filter(TestEvent::hasDocumentProcessed)
                .filter(e -> e.getDocumentProcessed().getSuccess())
                .count();

        // AssertJ: assertThat(processedCount).isEqualTo(50) -> Hamcrest: assertThat("Should process exactly 50 documents", processedCount, is(50L));
        assertThat("Should process exactly 50 documents", processedCount, is(50L));

        double docsPerSecond = (processedCount * 1000.0) / duration;
        LOG.infof("Bulk processing performance: %.2f docs/sec (processed %d docs in %d ms)",
                docsPerSecond, processedCount, duration);
    }

    @Disabled("Not implemented yet - will fix later.. we had this working")
    @Test
    void testChunkerDocuments() {
        // Test with chunked documents
        Collection<PipeDoc> chunkerDocs = testDataHelper.getChunkerPipeDocuments();
        
        assertThat("Chunker documents should be available", chunkerDocs, is(not(empty())));

        PipeDoc chunkedDoc = chunkerDocs.iterator().next();
        ModuleProcessRequest processRequest = createProcessRequest(chunkedDoc, "chunker-test");

        TestCommand command = TestCommand.newBuilder()
                .setCommandId("chunker-test-1")
                .setTimestamp(createTimestamp())
                .setProcessDocument(ProcessDocumentCommand.newBuilder()
                        .setRequest(processRequest)
                        .setExpectSuccess(true)
                        .setTimeoutMs(5000)
                        .build())
                .build();

        TestResult result = testHarness.executeTest(command)
                .await().atMost(Duration.ofSeconds(10));

        assertThat("Chunker document processing should succeed", result.getSuccess(), is(true));

        // Verify the document was processed with its chunk metadata intact
        TestEvent processedEvent = result.getEventsList().stream()
                .filter(TestEvent::hasDocumentProcessed)
                .findFirst()
                .orElse(null);

        assertThat("Chunker processed event should be present", processedEvent, is(notNullValue()));
        
        assertThat("Chunker document processing should succeed", processedEvent.getDocumentProcessed().getSuccess(), is(true));
        
        if (processedEvent.getDocumentProcessed().hasProcessingMetadata()) {
            // If the processor added metadata, verify it
            assertThat("Processing metadata should have fields", processedEvent.getDocumentProcessed().getProcessingMetadata().getFieldsCount(), is(greaterThan(0)));
        }
    }

    // Helper methods

    private ModuleProcessRequest createProcessRequest(PipeDoc doc, String pipelineName) {
        return ModuleProcessRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName(pipelineName)
                        .setPipeStepName("test-harness")
                        .setStreamId(UUID.randomUUID().toString())
                        .setCurrentHopNumber(1)
                        .putContextParams("test", "true")
                        .build())
                .build();
    }

    private com.google.protobuf.Timestamp createTimestamp() {
        long millis = System.currentTimeMillis();
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1000000))
                .build();
    }
}
