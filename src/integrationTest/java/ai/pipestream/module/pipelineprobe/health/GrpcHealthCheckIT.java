package ai.pipestream.module.pipelineprobe.health;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthGrpc;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.jboss.logging.Logger;

import java.net.URL;

/**
 * Integration test for gRPC health check using Quarkus-managed container.
 * This verifies health check works in production mode with dynamic port allocation.
 * Uses standard blocking stub to demonstrate protocol compatibility.
 */
@QuarkusIntegrationTest
public class GrpcHealthCheckIT extends GrpcHealthCheckTestBase {
    
    private static final Logger LOG = Logger.getLogger(GrpcHealthCheckIT.class);
    
    @TestHTTPResource
    URL testUrl;
    
    private ManagedChannel channel;
    private HealthGrpc.HealthBlockingStub healthService;
    
    @BeforeEach
    void setup() {
        // Extract host and port from the URL provided by Quarkus
        String host = testUrl.getHost();
        int port = testUrl.getPort();
        
        LOG.infof("Connecting to gRPC health service at %s:%d", host, port);
        
        channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        healthService = HealthGrpc.newBlockingStub(channel);
    }
    
    @AfterEach
    void cleanup() {
        if (channel != null) {
            channel.shutdown();
        }
    }
    
    @Override
    protected io.grpc.health.v1.HealthGrpc.HealthBlockingStub getHealthService() {
        return healthService;
    }
}