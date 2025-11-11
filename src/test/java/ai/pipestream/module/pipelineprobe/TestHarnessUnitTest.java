package ai.pipestream.module.pipelineprobe;


// import ai.pipestream.data.util.proto.ProtobufTestDataHelper;  // Temporarily disabled
import ai.pipestream.testing.harness.grpc.TestHarness;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;

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