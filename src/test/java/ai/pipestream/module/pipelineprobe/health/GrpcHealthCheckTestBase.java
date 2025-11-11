package ai.pipestream.module.pipelineprobe.health;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Base class for testing gRPC health check functionality.
 * This verifies that Quarkus gRPC health checks are properly configured and working.
 * Uses standard blocking stubs to prove any gRPC client can connect.
 */
public abstract class GrpcHealthCheckTestBase {
    
    /**
     * Subclasses must provide the health service client
     */
    protected abstract HealthGrpc.HealthBlockingStub getHealthService();
    
    @Test
    void testGrpcHealthCheckRespondsOk() {
        // Test default health check (empty service name)
        HealthCheckRequest request = HealthCheckRequest.newBuilder().build();
        
        HealthCheckResponse response = getHealthService().check(request);
        
        // Convert AssertJ to Hamcrest
        assertThat("Health response should not be null", response, is(notNullValue()));
        assertThat("Default health status should be SERVING", response.getStatus(), is(HealthCheckResponse.ServingStatus.SERVING));
    }
    
    @Test
    void testGrpcHealthCheckForSpecificService() {
        // Test health check for our specific gRPC service
        // Note: Quarkus gRPC health returns UNKNOWN for specific services by default
        HealthCheckRequest request = HealthCheckRequest.newBuilder()
            .setService("com.rokkon.pipeline.proto.PipeStepProcessor")
            .build();
            
        HealthCheckResponse response = getHealthService().check(request);
        
        // Convert AssertJ to Hamcrest
        assertThat("Specific service health response should not be null", response, is(notNullValue()));
        // Quarkus returns UNKNOWN for specific service queries unless explicitly registered
        assertThat("Specific service health status should be UNKNOWN", response.getStatus(), is(HealthCheckResponse.ServingStatus.UNKNOWN));
    }
    
    @Test
    void testGrpcHealthCheckForUnknownService() {
        // Test health check for non-existent service
        // Note: Quarkus gRPC health returns UNKNOWN status instead of throwing exception
        HealthCheckRequest request = HealthCheckRequest.newBuilder()
            .setService("com.example.NonExistentService")
            .build();
            
        HealthCheckResponse response = getHealthService().check(request);
        
        // Convert AssertJ to Hamcrest
        assertThat("Unknown service health response should not be null", response, is(notNullValue()));
        // Quarkus returns UNKNOWN for unregistered services
        assertThat("Unknown service health status should be UNKNOWN", response.getStatus(), is(HealthCheckResponse.ServingStatus.UNKNOWN));
    }
}