package com.td.mdcms.cdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.td.mdcms.cdb.exception.CdbFormatException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CdbBuilderTest {
    private static final String TEST_CDB_PREFIX = "test";
    private static final String TEST_CDB_TMP_PREFIX = "tmp-";
    private static final String TEST_CDB_SUFFIX = ".cdb";
    private static final String TEST_CDB_DUMP_SUFFIX = ".cdb-dump";
    private static final String TEST_CDB_RESOURCE_DIR = "cdb";

    private static final byte[] HAPPY_SIMPLE_KEY = "single".getBytes();
    private static final byte[] HAPPY_SIMPLE_DATA = "single data".getBytes();

    private static final String HAPPY_SIMPLE = "happy-simple";
    private static final String HAPPY_SIMPLE_INLINE_CR = "happy-simple-inline-cr";
    private static final String HAPPY_SIMPLE_INLINE_NL = "happy-simple-inline-nl";
    private static final String HAPPY_COMPLEX = "happy-complex";
    private static final String HAPPY_VERY_COMPLEX = "happy-very-complex";
    private static final String SAD_FORMAT_DATA_LENGTH = "sad-format-data-length";
    private static final String SAD_FORMAT_EOF = "sad-format-eof";
    private static final String SAD_FORMAT_KEY_LENGTH = "sad-format-key-length";
    private static final String SAD_FORMAT_KEY_TERMINATOR = "sad-format-key-terminator";
    private static final String SAD_FORMAT_LINE_TERMINATOR = "sad-format-line-terminator";
    private static final String SAD_FORMAT_PLUS = "sad-format-plus";
    private static final String SAD_FORMAT_SIZE_SEPARATOR = "sad-format-size-separator";
    private static final String SAD_FORMAT_SIZE_TERMINATOR = "sad-format-size-terminator";

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
    public void testCdbPathConstructorHappy() throws IOException {
        CdbBuilder builder = new CdbBuilder(cdbFilePath.get());
        builder.build();
        assertEquals(2048L, Files.size(cdbFilePath.get()));
        assertFalse(Files.exists(cdbTmpFilePath.get()));
    }

    @Test
    public void testCdbPathConstructorSimpleHappy()
            throws IOException, URISyntaxException {
        CdbBuilder builder = new CdbBuilder(cdbFilePath.get());
        // FIXME make it look like the simple dump
        builder.add(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA);
        builder.build();
        assertTrue(Files.exists(cdbFilePath.get()));
        assertFalse(Files.exists(cdbTmpFilePath.get()));

        assertCdbFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpSimpleHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE));
        CdbBuilder builder = new CdbBuilder(
                cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        builder.build();
        assertTrue(Files.exists(cdbFilePath.get()));
        assertFalse(Files.exists(cdbTmpFilePath.get()));

        assertCdbFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpSimpleInlineCrHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE_INLINE_CR));
        CdbBuilder builder = new CdbBuilder(
                cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        builder.build();
        assertTrue(Files.exists(cdbFilePath.get()));
        assertFalse(Files.exists(cdbTmpFilePath.get()));

        assertCdbFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE_INLINE_CR, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpSimpleInlineNlHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE_INLINE_NL));
        CdbBuilder builder = new CdbBuilder(
                cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        builder.build();
        assertTrue(Files.exists(cdbFilePath.get()));
        assertFalse(Files.exists(cdbTmpFilePath.get()));

        assertCdbFilesMatch(
                Path.of(getResourceUri(HAPPY_SIMPLE_INLINE_NL, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpComplexHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_COMPLEX));
        CdbBuilder builder = new CdbBuilder(
                cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        builder.build();
        assertTrue(Files.exists(cdbFilePath.get()));
        assertFalse(Files.exists(cdbTmpFilePath.get()));

        assertCdbFilesMatch(
                Path.of(getResourceUri(HAPPY_COMPLEX, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpVeryComplexHappy()
            throws IOException, URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_VERY_COMPLEX));
        CdbBuilder builder = new CdbBuilder(
                cdbFilePath.get(), cdbDumpPath, cdbTmpFilePath.get());
        builder.build();
        assertTrue(Files.exists(cdbFilePath.get()));
        assertFalse(Files.exists(cdbTmpFilePath.get()));

        assertCdbFilesMatch(
                Path.of(getResourceUri(HAPPY_VERY_COMPLEX, TEST_CDB_SUFFIX)),
                cdbFilePath.get());
    }

    @Test
    public void testCdbDumpFormatDataLengthSad() throws URISyntaxException {
        assertSadFormatException(
                SAD_FORMAT_DATA_LENGTH,
                cdbFilePath.get(), cdbTmpFilePath.get());
    }

    @Test
    public void testCdbDumpFormatNoEofSad() throws URISyntaxException {
        assertSadFormatException(
                SAD_FORMAT_EOF,
                cdbFilePath.get(), cdbTmpFilePath.get());
    }

    @Test
    public void testCdbDumpFormatKeyLengthSad() throws URISyntaxException {
        assertSadFormatException(
                SAD_FORMAT_KEY_LENGTH,
                cdbFilePath.get(), cdbTmpFilePath.get());
    }

    @Test
    public void testCdbDumpFormatKeyTerminatorSad() throws URISyntaxException {
        assertSadFormatException(
                SAD_FORMAT_KEY_TERMINATOR,
                cdbFilePath.get(), cdbTmpFilePath.get());
    }

    @Test
    public void testCdbDumpFormatLineTerminatorSad() throws URISyntaxException {
        assertSadFormatException(
                SAD_FORMAT_LINE_TERMINATOR,
                cdbFilePath.get(), cdbTmpFilePath.get());
    }

    @Test
    public void testCdbDumpFormatMissingPlusSad() throws URISyntaxException {
        assertSadFormatException(
                SAD_FORMAT_PLUS,
                cdbFilePath.get(), cdbTmpFilePath.get());
    }

    @Test
    public void testCdbDumpFormatSizeSeparatorSad() throws URISyntaxException {
        assertSadFormatException(
                SAD_FORMAT_SIZE_SEPARATOR,
                cdbFilePath.get(), cdbTmpFilePath.get());
    }

    @Test
    public void testCdbDumpFormatSizeTerminatorSad() throws URISyntaxException {
        assertSadFormatException(
                SAD_FORMAT_SIZE_TERMINATOR,
                cdbFilePath.get(), cdbTmpFilePath.get());
    }

    private URI getDumpResourceUri(String resourceName) throws URISyntaxException {
        return getResourceUri(resourceName, TEST_CDB_DUMP_SUFFIX);
    }

    private URI getResourceUri(String resourceName, String suffix) throws URISyntaxException {
        String resource = Path.of(TEST_CDB_RESOURCE_DIR, resourceName + suffix).toString();
        return getClass()
                .getClassLoader()
                .getResource(resource)
                .toURI();
    }

    private void assertCdbFilesMatch(Path expectedPath, Path actualPath) throws IOException {
        byte[] expected = Files.readAllBytes(expectedPath);
        byte[] actual = Files.readAllBytes(actualPath);
        assertArrayEquals(expected, actual);
    }

    private void assertSadFormatException(String sadCdbDump, Path cdbPath, Path cdbTmpPath)
            throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(sadCdbDump));
        assertThrows(CdbFormatException.class,
                () -> new CdbBuilder(cdbPath, cdbDumpPath, cdbTmpPath).build());
        assertTrue(Files.exists(cdbPath));
        assertFalse(Files.exists(cdbTmpPath));
    }
}
