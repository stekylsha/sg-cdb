package com.td.mdcms.cdb.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.td.mdcms.cdb.TestResources.HAPPY_COMPLEX;
import static com.td.mdcms.cdb.TestResources.HAPPY_MULTI_DATA_ONE;
import static com.td.mdcms.cdb.TestResources.HAPPY_MULTI_DATA_TWO;
import static com.td.mdcms.cdb.TestResources.HAPPY_MULTI_KEY;
import static com.td.mdcms.cdb.TestResources.HAPPY_SIMPLE;
import static com.td.mdcms.cdb.TestResources.HAPPY_SIMPLE_DATA;
import static com.td.mdcms.cdb.TestResources.HAPPY_SIMPLE_INLINE_CR;
import static com.td.mdcms.cdb.TestResources.HAPPY_SIMPLE_INLINE_NL;
import static com.td.mdcms.cdb.TestResources.HAPPY_SIMPLE_KEY;
import static com.td.mdcms.cdb.TestResources.HAPPY_VERY_COMPLEX;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_EOF;
import static com.td.mdcms.cdb.TestResources.TEST_CDB_PREFIX;
import static com.td.mdcms.cdb.TestResources.TEST_CDB_SUFFIX;
import static com.td.mdcms.cdb.TestResources.TEST_CDB_TMP_PREFIX;
import static com.td.mdcms.cdb.TestResources.assertFilesMatch;
import static com.td.mdcms.cdb.TestResources.getDumpResourceUri;
import static com.td.mdcms.cdb.TestResources.getResourceUri;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.td.mdcms.cdb.CdbBuilder;
import com.td.mdcms.cdb.exception.CdbFormatException;
import com.td.mdcms.cdb.model.ByteArrayPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CdbWriterTest {

    private ThreadLocal<Path> cdbFilePath;

    @BeforeEach
    public void setUp() throws IOException {
        Path cdbFile = Files.createTempFile(TEST_CDB_PREFIX, TEST_CDB_SUFFIX);

        cdbFilePath = new ThreadLocal<>();
        cdbFilePath.set(cdbFile);
    }

    @AfterEach
    public void cleanUp() throws IOException {
        Files.deleteIfExists(cdbFilePath.get());
    }

    @Test
    public void testEmptyHappy() throws IOException {
        CdbWriter cdbWriter = new CdbWriter(cdbFilePath.get());
        cdbWriter.close();
        assertEquals(2048L, Files.size(cdbFilePath.get()));
    }

    @Test
    public void testSimpleHappy()
            throws IOException, URISyntaxException {
        CdbWriter cdbWriter = new CdbWriter(cdbFilePath.get());
        cdbWriter.add(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA);
        cdbWriter.close();
        assertTrue(Files.exists(cdbFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testSimplePairHappy()
            throws IOException, URISyntaxException {
        CdbWriter cdbWriter = new CdbWriter(cdbFilePath.get());
        cdbWriter.add(new ByteArrayPair(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA));
        cdbWriter.close();
        assertTrue(Files.exists(cdbFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testComplexHappy()
            throws IOException, URISyntaxException {
        CdbWriter cdbWriter = new CdbWriter(cdbFilePath.get());
        cdbWriter.add(HAPPY_MULTI_KEY, HAPPY_MULTI_DATA_ONE);
        cdbWriter.add(HAPPY_MULTI_KEY, HAPPY_MULTI_DATA_TWO);
        cdbWriter.close();
        assertTrue(Files.exists(cdbFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_COMPLEX, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }
}
