package io.pipeline.module.pipelineprobe;

import com.google.protobuf.Empty;
import io.pipeline.data.v1.PipeDoc;
import io.pipeline.data.v1.SearchMetadata;
import io.pipeline.data.module.ModuleProcessRequest;
import io.pipeline.data.module.ServiceMetadata;
import io.pipeline.testing.harness.grpc.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.jboss.logging.Logger;

@QuarkusTest
class TestHarnessServiceTest {
    
    private static final Logger LOG = Logger.getLogger(TestHarnessServiceTest.class);
    
    @GrpcClient
    TestHarness testHarness;
    
    @Test
    void testModuleStatus() {
        // Given
        LOG.debug("Testing module status retrieval");
        
        // When
        ModuleStatus status = testHarness.getModuleStatus(Empty.getDefaultInstance())
                .await().atMost(Duration.ofSeconds(5));
        
        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(status).isNotNull() -> Hamcrest: assertThat("Status should not be null", status, is(notNullValue()))
        assertThat("Status should not be null", status, is(notNullValue()));
        
        // AssertJ: assertThat(status.getModuleName()).isEqualTo("test-processor") -> Hamcrest: assertThat("Module name should be 'test-processor'", status.getModuleName(), is("test-processor"))
        assertThat("Module name should be 'test-processor'", status.getModuleName(), is("test-processor"));
        
        // AssertJ: assertThat(status.getDocumentsProcessed()).isGreaterThanOrEqualTo(0) -> Hamcrest: assertThat("Documents processed should be >= 0", status.getDocumentsProcessed(), is(greaterThanOrEqualTo(0L)))
        assertThat("Documents processed should be >= 0", status.getDocumentsProcessed(), is(greaterThanOrEqualTo(0L)));
        
        // AssertJ: assertThat(status.getDocumentsFailed()).isGreaterThanOrEqualTo(0) -> Hamcrest: assertThat("Documents failed should be >= 0", status.getDocumentsFailed(), is(greaterThanOrEqualTo(0L)))
        assertThat("Documents failed should be >= 0", status.getDocumentsFailed(), is(greaterThanOrEqualTo(0L)));
        
        LOG.debugf("Module status test completed - module: %s, processed: %d, failed: %d", 
                  status.getModuleName(), status.getDocumentsProcessed(), status.getDocumentsFailed());
    }
    
    @Test
    void testSingleCommand() {
        // Given
        LOG.debug("Testing single health check command execution");
        
        TestCommand command = TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .build())
                .setCheckHealth(CheckHealthCommand.newBuilder()
                        .setIncludeDetails(true)
                        .build())
                .build();
        
        // When
        TestResult result = testHarness.executeTest(command)
                .await().atMost(Duration.ofSeconds(5));
        
        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(result.getSuccess()).isTrue() -> Hamcrest: assertThat("Test should be successful", result.getSuccess(), is(true))
        assertThat("Test should be successful", result.getSuccess(), is(true));
        
        // AssertJ: assertThat(result.getEventsCount()).isGreaterThan(0) -> Hamcrest: assertThat("Should have events", result.getEventsCount(), is(greaterThan(0)))
        assertThat("Should have events", result.getEventsCount(), is(greaterThan(0)));
        
        // Should have a health check event
        TestEvent healthEvent = result.getEventsList().stream()
                .filter(TestEvent::hasHealthCheck)
                .findFirst()
                .orElse(null);
        
        // AssertJ: assertThat(healthEvent).isNotNull() -> Hamcrest: assertThat("Health event should be present", healthEvent, is(notNullValue()))
        assertThat("Health event should be present", healthEvent, is(notNullValue()));
        
        // AssertJ: assertThat(healthEvent.getHealthCheck().getStatus()).isEqualTo(HealthCheckEvent.HealthStatus.HEALTHY) -> Hamcrest: assertThat("Health status should be HEALTHY", healthEvent.getHealthCheck().getStatus(), is(HealthCheckEvent.HealthStatus.HEALTHY))
        assertThat("Health status should be HEALTHY", healthEvent.getHealthCheck().getStatus(), is(HealthCheckEvent.HealthStatus.HEALTHY));
        
        LOG.debugf("Single command test completed - success: %s, events: %d", result.getSuccess(), result.getEventsCount());
    }
    
    @Test
    void testDocumentProcessing() {
        // Given
        LOG.debug("Testing document processing through test harness");
        
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Test Document")
                        .setBody("This is a test document for the harness")
                        .build())
                .build();
        
        ModuleProcessRequest processRequest = ModuleProcessRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-step")
                        .setStreamId(UUID.randomUUID().toString())
                        .setCurrentHopNumber(1)
                        .build())
                .build();
        
        TestCommand command = TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .build())
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
        // AssertJ: assertThat(result.getSuccess()).isTrue() -> Hamcrest: assertThat("Document processing should be successful", result.getSuccess(), is(true))
        assertThat("Document processing should be successful", result.getSuccess(), is(true));
        
        // Should have both received and processed events
        boolean hasReceivedEvent = result.getEventsList().stream()
                .anyMatch(TestEvent::hasDocumentReceived);
        boolean hasProcessedEvent = result.getEventsList().stream()
                .anyMatch(TestEvent::hasDocumentProcessed);
        
        // AssertJ: assertThat(hasReceivedEvent).isTrue() -> Hamcrest: assertThat("Should have document received event", hasReceivedEvent, is(true))
        assertThat("Should have document received event", hasReceivedEvent, is(true));
        
        // AssertJ: assertThat(hasProcessedEvent).isTrue() -> Hamcrest: assertThat("Should have document processed event", hasProcessedEvent, is(true))
        assertThat("Should have document processed event", hasProcessedEvent, is(true));
        
        // Check the processed event
        TestEvent processedEvent = result.getEventsList().stream()
                .filter(TestEvent::hasDocumentProcessed)
                .findFirst()
                .orElse(null);
        
        // AssertJ: assertThat(processedEvent).isNotNull() -> Hamcrest: assertThat("Processed event should be present", processedEvent, is(notNullValue()))
        assertThat("Processed event should be present", processedEvent, is(notNullValue()));
        
        // AssertJ: assertThat(processedEvent.getDocumentProcessed().getSuccess()).isTrue() -> Hamcrest: assertThat("Document processing should succeed", processedEvent.getDocumentProcessed().getSuccess(), is(true))
        assertThat("Document processing should succeed", processedEvent.getDocumentProcessed().getSuccess(), is(true));
        
        // AssertJ: assertThat(processedEvent.getDocumentProcessed().getDocumentId()).isEqualTo(testDoc.getDocId()) -> Hamcrest: assertThat("Document ID should match", processedEvent.getDocumentProcessed().getDocumentId(), is(testDoc.getDocId()))
        assertThat("Document ID should match", processedEvent.getDocumentProcessed().getDocumentId(), is(testDoc.getDocId()));
        
        LOG.debugf("Document processing test completed - document ID: %s, success: %s", 
                  testDoc.getDocId(), processedEvent.getDocumentProcessed().getSuccess());
    }
    
    @Test
    void testStreamingCommands() {
        // Given
        LOG.debug("Testing streaming commands with multiple operations");
        
        Multi<TestCommand> commands = Multi.createFrom().items(
                createHealthCheckCommand(),
                createDocumentProcessCommand(),
                createRegistrationCheckCommand()
        );
        
        // When
        AssertSubscriber<TestEvent> subscriber = testHarness.executeTestStream(commands)
                .subscribe().withSubscriber(AssertSubscriber.create(10));
        
        subscriber.awaitCompletion(Duration.ofSeconds(10));
        
        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(subscriber.getItems()).isNotEmpty() -> Hamcrest: assertThat("Should have received events", subscriber.getItems(), is(not(empty())))
        assertThat("Should have received events", subscriber.getItems(), is(not(empty())));
        
        // AssertJ: assertThat(subscriber.getFailure()).isNull() -> Hamcrest: assertThat("Stream should complete without failure", subscriber.getFailure(), is(nullValue()))
        assertThat("Stream should complete without failure", subscriber.getFailure(), is(nullValue()));
        
        // Should have events for each command
        long healthEvents = subscriber.getItems().stream()
                .filter(TestEvent::hasHealthCheck)
                .count();
        long processedEvents = subscriber.getItems().stream()
                .filter(TestEvent::hasDocumentProcessed)
                .count();
        long registrationEvents = subscriber.getItems().stream()
                .filter(TestEvent::hasModuleRegistered)
                .count();
        
        // AssertJ: assertThat(healthEvents).isGreaterThanOrEqualTo(1) -> Hamcrest: assertThat("Should have health events", healthEvents, is(greaterThanOrEqualTo(1L)))
        assertThat("Should have health events", healthEvents, is(greaterThanOrEqualTo(1L)));
        
        // AssertJ: assertThat(processedEvents).isGreaterThanOrEqualTo(1) -> Hamcrest: assertThat("Should have document processed events", processedEvents, is(greaterThanOrEqualTo(1L)))
        assertThat("Should have document processed events", processedEvents, is(greaterThanOrEqualTo(1L)));
        
        // AssertJ: assertThat(registrationEvents).isGreaterThanOrEqualTo(1) -> Hamcrest: assertThat("Should have registration events", registrationEvents, is(greaterThanOrEqualTo(1L)))
        assertThat("Should have registration events", registrationEvents, is(greaterThanOrEqualTo(1L)));
        
        LOG.debugf("Streaming test completed - health events: %d, processed events: %d, registration events: %d", 
                  healthEvents, processedEvents, registrationEvents);
    }
    
    private TestCommand createHealthCheckCommand() {
        return TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .build())
                .setCheckHealth(CheckHealthCommand.newBuilder()
                        .setIncludeDetails(false)
                        .build())
                .build();
    }
    
    private TestCommand createDocumentProcessCommand() {
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Stream Test Document")
                        .setBody("Testing streaming functionality")
                        .build())
                .build();
        
        ModuleProcessRequest processRequest = ModuleProcessRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("stream-test-pipeline")
                        .setPipeStepName("stream-test-step")
                        .setStreamId(UUID.randomUUID().toString())
                        .setCurrentHopNumber(1)
                        .build())
                .build();
        
        return TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .build())
                .setProcessDocument(ProcessDocumentCommand.newBuilder()
                        .setRequest(processRequest)
                        .setExpectSuccess(true)
                        .setTimeoutMs(5000)
                        .build())
                .build();
    }
    
    private TestCommand createRegistrationCheckCommand() {
        return TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .build())
                .setVerifyRegistration(VerifyRegistrationCommand.newBuilder()
                        .setExpectedModuleName("test-processor")
                        .setCheckConsul(false)
                        .build())
                .build();
    }
}