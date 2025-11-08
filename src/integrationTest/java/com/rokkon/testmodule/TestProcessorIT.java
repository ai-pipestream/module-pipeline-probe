package com.rokkon.testmodule;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pipeline.data.module.PipeStepProcessor;
import io.pipeline.data.module.PipeStepProcessorClient;
import io.pipeline.module.testharness.TestProcessorTestBase;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.jboss.logging.Logger;

/**
 * Integration test for TestProcessor.
 */
@QuarkusIntegrationTest
public class TestProcessorIT extends TestProcessorTestBase {
    
    private static final Logger LOG = Logger.getLogger(TestProcessorIT.class);

    @io.quarkus.test.common.http.TestHTTPResource
    java.net.URL testUrl;
    
    private ManagedChannel channel;
    private PipeStepProcessor pipeStepProcessor;

    @BeforeEach
    void setup() {
        // Extract host and port from the URL
        String host = testUrl.getHost();
        int port = testUrl.getPort();
        
        LOG.infof("Connecting to PipeStepProcessor at %s:%d", host, port);
        
        channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        pipeStepProcessor = new PipeStepProcessorClient("pipeStepProcessor", channel, (name, stub) -> stub);
    }

    @AfterEach
    void cleanup() {
        if (channel != null) {
            channel.shutdown();
        }
    }

    @Override
    protected PipeStepProcessor getTestProcessor() {
        return pipeStepProcessor;
    }
}