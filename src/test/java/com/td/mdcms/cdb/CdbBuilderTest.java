package com.td.mdcms.cdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.td.mdcms.cdb.TestResources.HAPPY_COMPLEX;
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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.td.mdcms.cdb.exception.CdbFormatException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdbBuilderTest {
    private static final Logger LOG = LoggerFactory.getLogger(CdbBuilderTest.class);

    private ThreadLocal<Path> cdbFilePath;
    private ThreadLocal<Path> cdbTmpFilePath;

    @BeforeEach
    public void setUp() throws IOException {
        Path cdbFile = Files.createTempFile(TEST_CDB_PREFIX, TEST_CDB_SUFFIX);
        Path cdbTmpFile = cdbFile.getParent().resolve(
                TEST_CDB_TMP_PREFIX + cdbFile.getFileName().toString());

        cdbFilePath = new ThreadLocal<>();
        cdbFilePath.set(cdbFile);

        cdbTmpFilePath = new ThreadLocal<>();
        cdbTmpFilePath.set(cdbTmpFile);
    }

    @AfterEach
    public void cleanUp() throws IOException {
        Files.deleteIfExists(cdbFilePath.get());
        Files.deleteIfExists(cdbTmpFilePath.get());
    }

    @Test
    public void testCdbDumpSimpleTwoParamHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE));
        CdbBuilder.build(cdbFilePath.get(), cdbDumpPath);
        assertTrue(Files.exists(cdbFilePath.get()));
        assertTrue(Files.notExists(cdbTmpFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpSimpleHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE));
        CdbBuilder.build(cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        assertTrue(Files.exists(cdbFilePath.get()));
        assertTrue(Files.notExists(cdbTmpFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpSimpleInlineCrHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE_INLINE_CR));
        CdbBuilder.build(cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        assertTrue(Files.exists(cdbFilePath.get()));
        assertTrue(Files.notExists(cdbTmpFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE_INLINE_CR, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpSimpleInlineNlHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE_INLINE_NL));
        CdbBuilder.build(cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        assertTrue(Files.exists(cdbFilePath.get()));
        assertTrue(Files.notExists(cdbTmpFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE_INLINE_NL, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpComplexHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_COMPLEX));
        CdbBuilder.build(cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        assertTrue(Files.exists(cdbFilePath.get()));
        assertTrue(Files.notExists(cdbTmpFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_COMPLEX, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpVeryComplexHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_VERY_COMPLEX));
        CdbBuilder.build(cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        assertTrue(Files.exists(cdbFilePath.get()));
        assertTrue(Files.notExists(cdbTmpFilePath.get()));

        assertFilesMatch(
                Path.of(getResourceUri(HAPPY_VERY_COMPLEX, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpFormatNoEofSad()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_EOF));
        Files.deleteIfExists(cdbFilePath.get());
        assertThrows(CdbFormatException.class,
                () -> CdbBuilder.build(
                        cdbFilePath.get(),
                        cdbDumpPath,
                        cdbTmpFilePath.get()));
        assertTrue(Files.notExists(cdbFilePath.get()));
        assertTrue(Files.notExists(cdbTmpFilePath.get()));
    }
}
