package ai.pipestream.module.pipelineprobe;

import ai.pipestream.data.util.proto.ProtobufTestDataHelper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ai.pipestream.testing.harness.grpc.TestHarness;
import ai.pipestream.testing.harness.grpc.TestHarnessClient;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.jboss.logging.Logger;

import java.net.URL;

/**
 * Integration test for TestHarness service.
 * This test runs against the containerized application.
 * <p>
 * Quarkus handles the container lifecycle and assigns a random available port.
 * We use the unified HTTP/gRPC server, so both protocols share the same port.
 */
@QuarkusIntegrationTest
class TestHarnessIT extends TestHarnessTestBase {
    
    private static final Logger LOG = Logger.getLogger(TestHarnessIT.class);

    // Quarkus provides the base URL for the running application
    @TestHTTPResource
    URL testUrl;
    
    private ManagedChannel channel;
    private TestHarness testHarnessClient;
    private final ProtobufTestDataHelper testDataHelper = new ProtobufTestDataHelper();

    @BeforeEach
    void setupChannel() {
        // Extract host and port from the URL
        String host = testUrl.getHost();
        int port = testUrl.getPort();
        
        LOG.infof("Connecting to TestHarness at %s:%d", host, port);
        
        // Create gRPC channel to the running container
        // Since we use unified server, gRPC is on the same port as HTTP
        channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
                
        // Create the TestHarness client
        testHarnessClient = new TestHarnessClient("TestHarness", channel, (name, stub) -> stub);
    }
    
    @AfterEach
    void teardownChannel() {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdownNow();
        }
    }

    @Override
    protected TestHarness getTestHarness() {
        return testHarnessClient;
    }
    
    @Override
    protected ProtobufTestDataHelper getTestDataHelper() {
        return testDataHelper;
    }
}