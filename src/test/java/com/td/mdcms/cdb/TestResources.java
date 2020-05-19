package com.td.mdcms.cdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.td.mdcms.cdb.model.ByteArrayPair;

public class TestResources {
    public static final String TEST_CDB_PREFIX = "test";
    public static final String TEST_CDB_TMP_PREFIX = "tmp-";
    public static final String TEST_CDB_SUFFIX = ".cdb";
    public static final String TEST_CDB_DUMP_SUFFIX = ".cdb-dump";
    public static final String TEST_CDB_RESOURCE_DIR = "cdb";

    public static final byte[] HAPPY_SIMPLE_KEY = "single".getBytes();
    public static final byte[] HAPPY_SIMPLE_DATA = "single data".getBytes();
    public static final byte[] HAPPY_SIMPLE_DATA_CR = "single\rdata".getBytes();
    public static final byte[] HAPPY_SIMPLE_DATA_NL = "single\ndata".getBytes();
    public static final byte[] HAPPY_MULTI_KEY = "multi".getBytes();
    public static final byte[] HAPPY_MULTI_DATA_ONE = "multi data 1".getBytes();
    public static final byte[] HAPPY_MULTI_DATA_TWO = "multi data 2".getBytes();
    public static final byte[] HAPPY_MULTI_1A_KEY = "multi 1a".getBytes();
    public static final byte[] HAPPY_MULTI_1A_DATA_ONE = "multi data 1a".getBytes();
    public static final byte[] HAPPY_MULTI_1A_DATA_TWO = "multi data 2a".getBytes();

    public static final String HAPPY_SIMPLE = "happy-simple";
    public static final String HAPPY_SIMPLE_INLINE_CR = "happy-simple-inline-cr";
    public static final String HAPPY_SIMPLE_INLINE_NL = "happy-simple-inline-nl";
    public static final String HAPPY_COMPLEX = "happy-complex";
    public static final String HAPPY_VERY_COMPLEX = "happy-very-complex";
    public static final String SAD_FORMAT_DATA_LENGTH = "sad-format-data-length";
    public static final String SAD_FORMAT_EOF = "sad-format-eof";
    public static final String SAD_FORMAT_KEY_LENGTH = "sad-format-key-length";
    public static final String SAD_FORMAT_KEY_TERMINATOR = "sad-format-key-terminator";
    public static final String SAD_FORMAT_LINE_TERMINATOR = "sad-format-line-terminator";
    public static final String SAD_FORMAT_PLUS = "sad-format-plus";
    public static final String SAD_FORMAT_SIZE_SEPARATOR = "sad-format-size-separator";
    public static final String SAD_FORMAT_SIZE_TERMINATOR = "sad-format-size-terminator";


    public static final List<ByteArrayPair> SIMPLE_DUMP_LIST = List.of(
            new ByteArrayPair(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA)
    );
    public static final List<ByteArrayPair> SIMPLE_CR_DUMP_LIST = List.of(
            new ByteArrayPair(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA_CR)
    );
    public static final List<ByteArrayPair> SIMPLE_NL_DUMP_LIST = List.of(
            new ByteArrayPair(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA_NL)
    );
    public static final List<ByteArrayPair> COMPLEX_DUMP_LIST = List.of(
            new ByteArrayPair(HAPPY_MULTI_KEY, HAPPY_MULTI_DATA_ONE),
            new ByteArrayPair(HAPPY_MULTI_KEY, HAPPY_MULTI_DATA_TWO)
    );
    public static final List<ByteArrayPair> VERY_COMPLEX_DUMP_LIST = List.of(
            new ByteArrayPair(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA),
            new ByteArrayPair(HAPPY_MULTI_KEY, HAPPY_MULTI_DATA_ONE),
            new ByteArrayPair(HAPPY_MULTI_KEY, HAPPY_MULTI_DATA_TWO),
            new ByteArrayPair(HAPPY_MULTI_1A_KEY, HAPPY_MULTI_1A_DATA_ONE),
            new ByteArrayPair(HAPPY_MULTI_1A_KEY, HAPPY_MULTI_1A_DATA_TWO)
    );

    public static URI getDumpResourceUri(String resourceName)
            throws URISyntaxException {
        return getResourceUri(resourceName, TEST_CDB_DUMP_SUFFIX);
    }

    public static URI getResourceUri(String resourceName, String suffix)
            throws URISyntaxException {
        String resource = Path.of(TEST_CDB_RESOURCE_DIR, resourceName + suffix).toString();
        return TestResources.class.getClassLoader()
                .getResource(resource)
                .toURI();
    }

    public static void assertFilesMatch(Path expectedPath, Path actualPath)
            throws IOException {
        byte[] expected = Files.readAllBytes(expectedPath);
        byte[] actual = Files.readAllBytes(actualPath);
        assertArrayEquals(expected, actual);
    }

}
