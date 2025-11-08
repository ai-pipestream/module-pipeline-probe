package com.rokkon.testmodule;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pipeline.data.model.PipeDoc;
import io.pipeline.data.module.ModuleProcessRequest;
import io.pipeline.data.module.ServiceMetadata;
import io.pipeline.testing.harness.grpc.*;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.smallrye.mutiny.Multi;
import io.quarkus.test.common.http.TestHTTPResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.jboss.logging.Logger;


@QuarkusIntegrationTest
class TestHarnessServiceIT {
    
    private static final Logger LOG = Logger.getLogger(TestHarnessServiceIT.class);
    
    @TestHTTPResource
    java.net.URL testUrl;
    
    private ManagedChannel channel;
    private TestHarnessClient testHarnessClient;

    @BeforeEach
    void setup() {
        // Extract host and port from the URL
        String host = testUrl.getHost();
        int port = testUrl.getPort();
        
        LOG.infof("Connecting to TestHarness service at %s:%d", host, port);
        
        channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        testHarnessClient = new TestHarnessClient("TestHarness", channel, (name, stub) -> stub);
    }
    
    @AfterEach
    void cleanup() {
        if (channel != null) {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
            }
        }
    }
    
    @Test
    void testModuleStatusInProdMode() {
        ModuleStatus status = testHarnessClient.getModuleStatus(Empty.getDefaultInstance())
                .await().atMost(Duration.ofSeconds(5));
        
        assertThat("Status should not be null", status, is(notNullValue()));
        assertThat("Module name should be test-processor", status.getModuleName(), is("test-processor"));
        // In prod mode, module should be functioning
        assertThat("Module should have last activity", status.hasLastActivity(), is(true));
    }
    
    @Test
    void testStreamingWithMultipleClients() throws InterruptedException {
        // Simulate multiple clients connecting with concurrent streams
        CountDownLatch latch = new CountDownLatch(2);
        List<TestEvent> stream1Events = new ArrayList<>();
        List<TestEvent> stream2Events = new ArrayList<>();
        
        // Stream 1
        Multi<TestCommand> commands1 = Multi.createFrom().items(
                createDocumentProcessCommand("stream1-doc"),
                createHealthCheckCommand()
        );
        
        testHarnessClient.executeTestStream(commands1)
                .subscribe().with(
                        stream1Events::add,
                        error -> latch.countDown(),
                        latch::countDown
                );
        
        // Stream 2 - Using the same client but different stream
        Multi<TestCommand> commands2 = Multi.createFrom().items(
                createDocumentProcessCommand("stream2-doc"),
                createHealthCheckCommand()
        );
        
        testHarnessClient.executeTestStream(commands2)
                .subscribe().with(
                        stream2Events::add,
                        error -> latch.countDown(),
                        latch::countDown
                );
        
        // Wait for both streams to complete
        assertThat("Latch should countdown within 10 seconds", latch.await(10, TimeUnit.SECONDS), is(true));
        
        // Both streams should have received events
        assertThat("Stream 1 should have events", stream1Events, is(not(empty())));
        assertThat("Stream 2 should have events", stream2Events, is(not(empty())));
        
        // Each stream should have events for their own documents
        boolean stream1HasOwnDoc = stream1Events.stream()
                .filter(TestEvent::hasDocumentProcessed)
                .anyMatch(e -> e.getDocumentProcessed().getDocumentId().equals("stream1-doc"));
        
        boolean stream2HasOwnDoc = stream2Events.stream()
                .filter(TestEvent::hasDocumentProcessed)
                .anyMatch(e -> e.getDocumentProcessed().getDocumentId().equals("stream2-doc"));
        
        assertThat("Stream 1 should have processed its own document", stream1HasOwnDoc, is(true));
        assertThat("Stream 2 should have processed its own document", stream2HasOwnDoc, is(true));
    }
    
    @Test
    void testErrorHandling() {
        // Create a command that will cause an error
        TestCommand errorCommand = TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .build())
                .setProcessDocument(ProcessDocumentCommand.newBuilder()
                        .setRequest(ModuleProcessRequest.newBuilder()
                                // No document - might cause issues
                                .setMetadata(ServiceMetadata.newBuilder()
                                        .setPipelineName("error-test")
                                        .setPipeStepName("error-step")
                                        .setStreamId(UUID.randomUUID().toString())
                                        .setCurrentHopNumber(1)
                                        .build())
                                .build())
                        .setExpectSuccess(false)
                        .setTimeoutMs(5000)
                        .build())
                .build();
        
        TestResult result = testHarnessClient.executeTest(errorCommand)
                .await().atMost(Duration.ofSeconds(5));
        
        assertThat("Result should not be null", result, is(notNullValue()));
        // Even with no document, the harness should handle it gracefully
        assertThat("Error handling should be successful", result.getSuccess(), is(true));
    }
    
    @Test
    void testModuleConfiguration() {
        // Configure the module
        TestCommand configCommand = TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .build())
                .setConfigureModule(ConfigureModuleCommand.newBuilder()
                        .setConfig(com.google.protobuf.Struct.newBuilder()
                                .putFields("test_mode", com.google.protobuf.Value.newBuilder()
                                        .setStringValue("integration-test")
                                        .build())
                                .putFields("delay_ms", com.google.protobuf.Value.newBuilder()
                                        .setNumberValue(100)
                                        .build())
                                .build())
                        .setResetToDefaults(false)
                        .build())
                .build();
        
        TestResult result = testHarnessClient.executeTest(configCommand)
                .await().atMost(Duration.ofSeconds(5));
        
        assertThat("Module configuration should be successful", result.getSuccess(), is(true));
        
        // Check that configuration was applied
        ModuleStatus status = testHarnessClient.getModuleStatus(Empty.getDefaultInstance())
                .await().atMost(Duration.ofSeconds(5));
        
        assertThat("Config map should contain test_mode key", status.getCurrentConfigMap(), hasKey("test_mode"));
        assertThat("test_mode value should be integration-test", status.getCurrentConfigMap().get("test_mode"), is("integration-test"));
    }
    
    private TestCommand createHealthCheckCommand() {
        return TestCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .build())
                .setCheckHealth(CheckHealthCommand.newBuilder()
                        .setIncludeDetails(true)
                        .build())
                .build();
    }
    
    private TestCommand createDocumentProcessCommand(String docId) {
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setId(docId)
                .setTitle("Integration Test Document")
                .setBody("Testing in production mode")
                .build();
        
        ModuleProcessRequest processRequest = ModuleProcessRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("integration-test-pipeline")
                        .setPipeStepName("integration-test-step")
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
}