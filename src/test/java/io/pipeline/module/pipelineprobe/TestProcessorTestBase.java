package io.pipeline.module.pipelineprobe;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.pipeline.data.v1.PipeDoc;
import io.pipeline.data.v1.SearchMetadata;
import io.pipeline.data.module.*;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


/**
 * Base test class for TestProcessor service testing.
 * This abstract class can be extended by both unit tests (@QuarkusTest) 
 * and integration tests (@QuarkusIntegrationTest).
 */
public abstract class TestProcessorTestBase {

    private static final Logger LOG = Logger.getLogger(TestProcessorTestBase.class);

    protected abstract PipeStepProcessor getTestProcessor();

    @Test
    void testProcessData() {
        // Create test document
        PipeDoc document = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Test Document")
                        .setBody("This is test content for the TestProcessor")
                        .setCustomFields(Struct.newBuilder()
                                .putFields("source", Value.newBuilder().setStringValue("test").build())
                                .build())
                        .build())
                .build();

        // Create request with metadata
        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(document)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .putConfigParams("mode", "test")
                        .putConfigParams("addMetadata", "true")
                        .build())
                .build();

        // Process and verify
        LOG.debugf("Sending test request with document: %s", document.getDocId());

        UniAssertSubscriber<ModuleProcessResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ModuleProcessResponse response = subscriber.awaitItem().getItem();

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(response).isNotNull() -> Hamcrest: assertThat("Response should not be null", response, is(notNullValue()))
        assertThat("Response should not be null", response, is(notNullValue()));
        
        // AssertJ: assertThat(response.getSuccess()).isTrue() -> Hamcrest: assertThat("Processing should be successful", response.getSuccess(), is(true))
        assertThat("Processing should be successful", response.getSuccess(), is(true));
        
        // AssertJ: assertThat(response.hasOutputDoc()).isTrue() -> Hamcrest: assertThat("Response should have output document", response.hasOutputDoc(), is(true))
        assertThat("Response should have output document", response.hasOutputDoc(), is(true));
        
        // AssertJ: assertThat(response.getOutputDoc().getDocId()).isEqualTo(document.getDocId()) -> Hamcrest: assertThat("Output document ID should match input", response.getOutputDoc().getDocId(), is(document.getDocId()))
        assertThat("Output document ID should match input", response.getOutputDoc().getDocId(), is(document.getDocId()));

        // Verify custom_data was enhanced
        Struct customData = response.getOutputDoc().getSearchMetadata().getCustomFields();
        
        // AssertJ: assertThat(customData.getFieldsMap()).containsKey("processed_by") -> Hamcrest: assertThat("Custom data should contain processed_by", customData.getFieldsMap(), hasKey("processed_by"))
        assertThat("Custom data should contain processed_by", customData.getFieldsMap(), hasKey("processed_by"));
        
        // AssertJ: assertThat(customData.getFieldsMap()).containsKey("processing_timestamp") -> Hamcrest: assertThat("Custom data should contain processing_timestamp", customData.getFieldsMap(), hasKey("processing_timestamp"))
        assertThat("Custom data should contain processing_timestamp", customData.getFieldsMap(), hasKey("processing_timestamp"));
        
        // AssertJ: assertThat(customData.getFieldsMap()).containsKey("test_module_version") -> Hamcrest: assertThat("Custom data should contain test_module_version", customData.getFieldsMap(), hasKey("test_module_version"))
        assertThat("Custom data should contain test_module_version", customData.getFieldsMap(), hasKey("test_module_version"));
        
        // AssertJ: assertThat(customData.getFieldsMap()).containsKey("config_mode") -> Hamcrest: assertThat("Custom data should contain config_mode", customData.getFieldsMap(), hasKey("config_mode"))
        assertThat("Custom data should contain config_mode", customData.getFieldsMap(), hasKey("config_mode"));
        
        // AssertJ: assertThat(customData.getFieldsMap()).containsKey("config_addMetadata") -> Hamcrest: assertThat("Custom data should contain config_addMetadata", customData.getFieldsMap(), hasKey("config_addMetadata"))
        assertThat("Custom data should contain config_addMetadata", customData.getFieldsMap(), hasKey("config_addMetadata"));

        // Verify logs
        // AssertJ: assertThat(response.getProcessorLogsList()).isNotEmpty() -> Hamcrest: assertThat("Should have processor logs", response.getProcessorLogsList(), is(not(empty())))
        assertThat("Should have processor logs", response.getProcessorLogsList(), is(not(empty())));
        
        // AssertJ: assertThat(response.getProcessorLogsList()).anyMatch(log -> log.contains("TestProcessor: Document processed successfully")) -> Hamcrest: assertThat("Should contain success log message", response.getProcessorLogsList(), hasItem(containsString("TestProcessor: Document processed successfully")))
        assertThat("Should contain success log message", response.getProcessorLogsList(), hasItem(containsString("TestProcessor: Document processed successfully")));

        LOG.debugf("Test completed successfully, response: %s", response.getSuccess());
    }

    @Test
    void testProcessDataWithoutDocument() {
        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .build();

        LOG.debugf("Sending test request without document");

        UniAssertSubscriber<ModuleProcessResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ModuleProcessResponse response = subscriber.awaitItem().getItem();

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(response).isNotNull() -> Hamcrest: assertThat("Response should not be null", response, is(notNullValue()))
        assertThat("Response should not be null", response, is(notNullValue()));
        
        // AssertJ: assertThat(response.getSuccess()).isTrue() -> Hamcrest: assertThat("Processing without document should succeed", response.getSuccess(), is(true))
        assertThat("Processing without document should succeed", response.getSuccess(), is(true));
        
        // AssertJ: assertThat(response.hasOutputDoc()).isFalse() -> Hamcrest: assertThat("Should not have output document when no input", response.hasOutputDoc(), is(false))
        assertThat("Should not have output document when no input", response.hasOutputDoc(), is(false));
        
        // AssertJ: assertThat(response.getProcessorLogsList()).anyMatch(log -> log.contains("TestProcessor: No document provided")) -> Hamcrest: assertThat("Should contain no document log message", response.getProcessorLogsList(), hasItem(containsString("TestProcessor: No document provided")))
        assertThat("Should contain no document log message", response.getProcessorLogsList(), hasItem(containsString("TestProcessor: No document provided")));

        LOG.debugf("Test without document completed successfully");
    }

    @Test
    void testGetServiceRegistration() {
        LOG.debugf("Testing service registration");

        UniAssertSubscriber<ServiceRegistrationMetadata> subscriber = getTestProcessor()
                .getServiceRegistration(RegistrationRequest.newBuilder().build())
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ServiceRegistrationMetadata registration = subscriber.awaitItem().getItem();

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(registration).isNotNull() -> Hamcrest: assertThat("Registration response should not be null", registration, is(notNullValue()))
        assertThat("Registration response should not be null", registration, is(notNullValue()));
        
        // AssertJ: assertThat(registration.getModuleName()).isNotEmpty() -> Hamcrest: assertThat("Module name should not be empty", registration.getModuleName(), is(not(emptyString())))
        assertThat("Module name should not be empty", registration.getModuleName(), is(not(emptyString())));
        
        // AssertJ: assertThat(registration.getJsonConfigSchema()).isNotEmpty() -> Hamcrest: assertThat("JSON config schema should not be empty", registration.getJsonConfigSchema(), is(not(emptyString())))
        assertThat("JSON config schema should not be empty", registration.getJsonConfigSchema(), is(not(emptyString())));

        // Verify the schema is valid JSON
        // AssertJ: assertThat(registration.getJsonConfigSchema()).contains("\"type\": \"object\"") -> Hamcrest: assertThat("Schema should contain type object", registration.getJsonConfigSchema(), containsString("\"type\": \"object\""))
        assertThat("Schema should contain type object", registration.getJsonConfigSchema(), containsString("\"type\": \"object\""));
        
        // AssertJ: assertThat(registration.getJsonConfigSchema()).contains("\"properties\"") -> Hamcrest: assertThat("Schema should contain properties", registration.getJsonConfigSchema(), containsString("\"properties\""))
        assertThat("Schema should contain properties", registration.getJsonConfigSchema(), containsString("\"properties\""));
        
        // AssertJ: assertThat(registration.getJsonConfigSchema()).contains("mode") -> Hamcrest: assertThat("Schema should contain mode property", registration.getJsonConfigSchema(), containsString("mode"))
        assertThat("Schema should contain mode property", registration.getJsonConfigSchema(), containsString("mode"));
        
        // AssertJ: assertThat(registration.getJsonConfigSchema()).contains("addMetadata") -> Hamcrest: assertThat("Schema should contain addMetadata property", registration.getJsonConfigSchema(), containsString("addMetadata"))
        assertThat("Schema should contain addMetadata property", registration.getJsonConfigSchema(), containsString("addMetadata"));
        
        // AssertJ: assertThat(registration.getJsonConfigSchema()).contains("simulateError") -> Hamcrest: assertThat("Schema should contain simulateError property", registration.getJsonConfigSchema(), containsString("simulateError"))
        assertThat("Schema should contain simulateError property", registration.getJsonConfigSchema(), containsString("simulateError"));

        LOG.debugf("Service registration test completed, module name: %s", registration.getModuleName());
    }

    @Test
    void testProcessDataWithDelay() {
        // Create test document
        PipeDoc document = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Delay Test Document")
                        .setBody("Testing processing delay")
                        .build())
                .build();

        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(document)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor-delay")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .build();

        LOG.debugf("Sending test request with delay");

        long startTime = System.currentTimeMillis();

        UniAssertSubscriber<ModuleProcessResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ModuleProcessResponse response = subscriber.awaitItem().getItem();

        long endTime = System.currentTimeMillis();

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(response).isNotNull() -> Hamcrest: assertThat("Delay test response should not be null", response, is(notNullValue()))
        assertThat("Delay test response should not be null", response, is(notNullValue()));
        
        // AssertJ: assertThat(response.getSuccess()).isTrue() -> Hamcrest: assertThat("Delay test processing should succeed", response.getSuccess(), is(true))
        assertThat("Delay test processing should succeed", response.getSuccess(), is(true));
        
        // AssertJ: assertThat(response.hasOutputDoc()).isTrue() -> Hamcrest: assertThat("Delay test should have output document", response.hasOutputDoc(), is(true))
        assertThat("Delay test should have output document", response.hasOutputDoc(), is(true));

        LOG.debugf("Test with delay completed in %d ms", endTime - startTime);
    }

    @Test
    void testSchemaValidationMode() {
        // Test with valid document
        PipeDoc validDocument = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Valid Document")
                        .setBody("This document has all required fields")
                        .build())
                .build();

        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(validDocument)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setCustomJsonConfig(Struct.newBuilder()
                                .putFields("mode", Value.newBuilder().setStringValue("validate").build())
                                .build())
                        .build())
                .build();

        LOG.debugf("Testing schema validation with valid document");

        UniAssertSubscriber<ModuleProcessResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ModuleProcessResponse response = subscriber.awaitItem().getItem();

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(response).isNotNull() -> Hamcrest: assertThat("Schema validation response should not be null", response, is(notNullValue()))
        assertThat("Schema validation response should not be null", response, is(notNullValue()));
        
        // AssertJ: assertThat(response.getSuccess()).isTrue() -> Hamcrest: assertThat("Schema validation should succeed", response.getSuccess(), is(true))
        assertThat("Schema validation should succeed", response.getSuccess(), is(true));
        
        // AssertJ: assertThat(response.getProcessorLogsList()).anyMatch(log -> log.contains("Schema validation passed")) -> Hamcrest: assertThat("Should contain schema validation passed log", response.getProcessorLogsList(), hasItem(containsString("Schema validation passed")))
        assertThat("Should contain schema validation passed log", response.getProcessorLogsList(), hasItem(containsString("Schema validation passed")));

        LOG.debugf("Schema validation test with valid document passed");
    }

    @Test
    void testSchemaValidationModeWithMissingTitle() {
        // Test with document missing title
        PipeDoc invalidDocument = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("This document is missing title")
                        .build())
                .build();

        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(invalidDocument)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setCustomJsonConfig(Struct.newBuilder()
                                .putFields("mode", Value.newBuilder().setStringValue("validate").build())
                                .build())
                        .build())
                .build();

        LOG.debugf("Testing schema validation with missing title");

        UniAssertSubscriber<ModuleProcessResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ModuleProcessResponse response = subscriber.awaitItem().getItem();

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(response).isNotNull() -> Hamcrest: assertThat("Invalid document response should not be null", response, is(notNullValue()))
        assertThat("Invalid document response should not be null", response, is(notNullValue()));
        
        // AssertJ: assertThat(response.getSuccess()).isFalse() -> Hamcrest: assertThat("Invalid document processing should fail", response.getSuccess(), is(false))
        assertThat("Invalid document processing should fail", response.getSuccess(), is(false));
        
        // AssertJ: assertThat(response.hasErrorDetails()).isTrue() -> Hamcrest: assertThat("Should have error details", response.hasErrorDetails(), is(true))
        assertThat("Should have error details", response.hasErrorDetails(), is(true));
        
        // AssertJ: assertThat(response.getErrorDetails().getFieldsMap().get("error_message").getStringValue()).contains("Schema validation failed: title is required") -> Hamcrest: assertThat("Error message should contain schema validation failure", response.getErrorDetails().getFieldsMap().get("error_message").getStringValue(), containsString("Schema validation failed: title is required"))
        assertThat("Error message should contain schema validation failure", response.getErrorDetails().getFieldsMap().get("error_message").getStringValue(), containsString("Schema validation failed: title is required"));

        LOG.debugf("Schema validation test with missing title correctly failed");
    }

    @Test
    void testSchemaValidationWithRequireSchemaFlag() {
        // Test requireSchema flag overrides mode
        PipeDoc validDocument = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Valid Document")
                        .setBody("Testing requireSchema flag")
                        .build())
                .build();

        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(validDocument)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setCustomJsonConfig(Struct.newBuilder()
                                .putFields("mode", Value.newBuilder().setStringValue("test").build())
                                .putFields("requireSchema", Value.newBuilder().setBoolValue(true).build())
                                .build())
                        .build())
                .build();

        LOG.debugf("Testing requireSchema flag");

        UniAssertSubscriber<ModuleProcessResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ModuleProcessResponse response = subscriber.awaitItem().getItem();

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(response).isNotNull() -> Hamcrest: assertThat("RequireSchema flag response should not be null", response, is(notNullValue()))
        assertThat("RequireSchema flag response should not be null", response, is(notNullValue()));
        
        // AssertJ: assertThat(response.getSuccess()).isTrue() -> Hamcrest: assertThat("RequireSchema flag processing should succeed", response.getSuccess(), is(true))
        assertThat("RequireSchema flag processing should succeed", response.getSuccess(), is(true));
        
        // AssertJ: assertThat(response.getProcessorLogsList()).anyMatch(log -> log.contains("Schema validation passed")) -> Hamcrest: assertThat("Should contain schema validation passed log from requireSchema flag", response.getProcessorLogsList(), hasItem(containsString("Schema validation passed")))
        assertThat("Should contain schema validation passed log from requireSchema flag", response.getProcessorLogsList(), hasItem(containsString("Schema validation passed")));

        LOG.debugf("RequireSchema flag test passed");
    }

    @Test
    void testSimulateError() {
        PipeDoc document = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Error Test Document")
                        .setBody("Testing error simulation")
                        .build())
                .build();

        ModuleProcessRequest request = ModuleProcessRequest.newBuilder()
                .setDocument(document)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("test-processor")
                        .setStreamId("test-stream-1")
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setCustomJsonConfig(Struct.newBuilder()
                                .putFields("simulateError", Value.newBuilder().setBoolValue(true).build())
                                .build())
                        .build())
                .build();

        LOG.debugf("Testing error simulation");

        UniAssertSubscriber<ModuleProcessResponse> subscriber = getTestProcessor()
                .processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ModuleProcessResponse response = subscriber.awaitItem().getItem();

        // Then - Convert AssertJ to Hamcrest with detailed descriptions
        // AssertJ: assertThat(response).isNotNull() -> Hamcrest: assertThat("Error simulation response should not be null", response, is(notNullValue()))
        assertThat("Error simulation response should not be null", response, is(notNullValue()));
        
        // AssertJ: assertThat(response.getSuccess()).isFalse() -> Hamcrest: assertThat("Error simulation should fail", response.getSuccess(), is(false))
        assertThat("Error simulation should fail", response.getSuccess(), is(false));
        
        // AssertJ: assertThat(response.hasErrorDetails()).isTrue() -> Hamcrest: assertThat("Error simulation should have error details", response.hasErrorDetails(), is(true))
        assertThat("Error simulation should have error details", response.hasErrorDetails(), is(true));
        
        // AssertJ: assertThat(response.getErrorDetails().getFieldsMap().get("error_message").getStringValue()).contains("Simulated error for testing") -> Hamcrest: assertThat("Error message should contain simulation text", response.getErrorDetails().getFieldsMap().get("error_message").getStringValue(), containsString("Simulated error for testing"))
        assertThat("Error message should contain simulation text", response.getErrorDetails().getFieldsMap().get("error_message").getStringValue(), containsString("Simulated error for testing"));

        LOG.debugf("Error simulation test passed");
    }
}
