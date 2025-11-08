package io.pipeline.module.pipelineprobe;


// import io.pipeline.data.util.proto.ProtobufTestDataHelper;  // Temporarily disabled
import io.pipeline.testing.harness.grpc.TestHarness;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

/**
 * Unit test for TestHarness using Quarkus Test framework.
 */
@QuarkusTest
class TestHarnessUnitTest extends TestHarnessTestBase {

    @GrpcClient
    TestHarness testHarness;

    // @Inject
    // ProtobufTestDataHelper testDataHelper;

    @Override
    protected TestHarness getTestHarness() {
        return testHarness;
    }
    
    // No need to override getTestDataHelper() - base class provides default implementation
}