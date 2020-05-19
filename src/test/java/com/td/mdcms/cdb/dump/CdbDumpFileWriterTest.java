package com.td.mdcms.cdb.dump;

import static com.td.mdcms.cdb.TestResources.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.td.mdcms.cdb.model.ByteArrayPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdbDumpFileWriterTest {
    private static final Logger LOG = LoggerFactory.getLogger(CdbDumpFileWriterTest.class);

    private static final String TEST_TMP_PREFIX = "test.tmp-";
    private static final String TEST_TMP_DUMP_SUFFIX = ".cdb-dump";

    private ThreadLocal<Path> cdbDumpPath;

    @BeforeEach
    public void setUp() throws IOException {
        cdbDumpPath = new ThreadLocal<>();
        cdbDumpPath.set(Files.createTempFile(TEST_TMP_PREFIX, TEST_TMP_DUMP_SUFFIX));
    }

    @AfterEach
    public void cleanUp() throws IOException {
        Files.deleteIfExists(cdbDumpPath.get());
    }

    @Test
    public void openOutputNotExists() throws IOException {
        Files.deleteIfExists(cdbDumpPath.get());
        CdbDumpFileWriter cdfw = new CdbDumpFileWriter(cdbDumpPath.get());
        cdfw.writeDumpElement(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA);
        cdfw.close();
    }

    @Test
    public void writeSimpleDumpBytes() throws IOException, URISyntaxException {
        CdbDumpFileWriter cdfw = new CdbDumpFileWriter(cdbDumpPath.get());
        cdfw.writeDumpElement(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA);
        cdfw.close();

        Path expectedPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE));
        assertFilesMatch(expectedPath, cdbDumpPath.get());
    }

    @Test
    public void writeSimpleDumpByteArrayPair() throws IOException, URISyntaxException {
        CdbDumpFileWriter cdfw = new CdbDumpFileWriter(cdbDumpPath.get());
        cdfw.writeDumpElement(new ByteArrayPair(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA));
        cdfw.close();

        Path expectedPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE));
        assertFilesMatch(expectedPath, cdbDumpPath.get());
    }

    @Test
    public void writeComplexDump() throws IOException, URISyntaxException {
        CdbDumpFileWriter cdfw = new CdbDumpFileWriter(cdbDumpPath.get());
        VERY_COMPLEX_DUMP_LIST.forEach(bap -> cdfw.writeDumpElement(bap));
        cdfw.close();

        Path expectedPath = Path.of(getDumpResourceUri(HAPPY_VERY_COMPLEX));
        assertFilesMatch(expectedPath, cdbDumpPath.get());
    }
}
