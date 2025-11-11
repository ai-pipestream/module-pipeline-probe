package ai.pipestream.module.pipelineprobe;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Custom test data helper for module-pipeline-probe tests
 * Provides mock data for testing without external dependencies
 */
class TestDataHelper {

    Collection<PipeDoc> getSamplePipeDocuments() {
        List<PipeDoc> docs = new ArrayList<>();
        // Generate 100 sample documents for bulk processing tests
        for (int i = 1; i <= 100; i++) {
            docs.add(createTestDoc("test-doc-" + i, "Sample Document " + i,
                "This is sample document content " + i + ". This document contains some test data for processing."));
        }
        return docs;
    }

    Collection<PipeDoc> getTikaPipeDocuments() {
        return List.of(
            createTestDoc("tika-doc-1", "TIKA Document 1", "Parsed content from TIKA processing 1"),
            createTestDoc("tika-doc-2", "TIKA Document 2", "Parsed content from TIKA processing 2")
        );
    }

    Collection<PipeDoc> getChunkerPipeDocuments() {
        return List.of(
            createTestDoc("chunk-doc-1", "Chunk Document 1", "This document will be chunked into smaller pieces"),
            createTestDoc("chunk-doc-2", "Chunk Document 2", "Another document for chunking tests")
        );
    }

    Collection<PipeStream> getSamplePipeStreams() {
        return List.of(
            PipeStream.newBuilder()
                .setStreamId("test-stream-1")
                .setDocument(createTestDoc("stream-doc-1", "Stream Document", "Document in a stream"))
                .build(),
            PipeStream.newBuilder()
                .setStreamId("test-stream-2")
                .setDocument(createTestDoc("stream-doc-2", "Another Stream Doc", "Another document in stream"))
                .build()
        );
    }

    private PipeDoc createTestDoc(String id, String title, String body) {
        return PipeDoc.newBuilder()
            .setDocId(id)
            .setSearchMetadata(
                ai.pipestream.data.v1.SearchMetadata.newBuilder()
                    .setTitle(title)
                    .setBody(body)
                    .build()
            )
            .build();
    }
}

