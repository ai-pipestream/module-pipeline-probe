package ai.pipestream.module.pipelineprobe;

import ai.pipestream.data.module.PipeStepProcessor;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Unit test for TestProcessor using Quarkus Test framework.
 */
@QuarkusTest
class TestProcessorUnitTest extends TestProcessorTestBase {

    @GrpcClient
    PipeStepProcessor pipeStepProcessor;

    @Override
    protected PipeStepProcessor getTestProcessor() {
        return pipeStepProcessor;
    }
}