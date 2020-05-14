package com.td.mdcms.cdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.td.mdcms.cdb.exception.CdbException;
import com.td.mdcms.cdb.model.ByteArrayPair;
import com.td.mdcms.cdb.model.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class CdbTest {

    private static final String TEST_CDB_PREFIX = "test";
    private static final String TEST_CDB_SUFFIX = ".cdb";
    private static final byte[] TEST_KEY_SINGLE = "single".getBytes();
    private static final byte[] TEST_DATA_SINGLE = "single data".getBytes();
    private static final byte[] TEST_KEY_MULTI = "multi".getBytes();
    private static final byte[] TEST_DATA_MULTI_ONE = "multi data 1".getBytes();
    private static final byte[] TEST_DATA_MULTI_TWO = "multi data 2".getBytes();
    private static final List<byte[]> TEST_DATA_MULTI_LIST =
            List.of(TEST_DATA_MULTI_ONE, TEST_DATA_MULTI_TWO);

    private ThreadLocal<Path> cdbFilePath;

    @BeforeEach
    public void setUp() throws IOException {
        cdbFilePath = new ThreadLocal<>();
        cdbFilePath.set(Files.createTempFile(
                TEST_CDB_PREFIX,
                TEST_CDB_SUFFIX));
    }

    @AfterEach
    public void cleanUp() throws IOException {
        Files.deleteIfExists(cdbFilePath.get());
    }

    /**
     * Everything works as expected.
     * @throws IOException If the setup fails.
     */
    @Test
    public void testHappyOpenClose() throws IOException {
        Path cdbFilePath = createEmptyCdb();
        Cdb cdb = null;
        try {
            cdb = new Cdb(cdbFilePath);
        } finally {
            if (cdb != null) {
                cdb.close();
            }
        }
    }

    /**
     * The cdb file does not exist. A {@link CdbException} is expected.
     */
    @Test
    public void testSadOpenNoFile() throws IOException {
        assertThrows(CdbException.class, () -> {
            Cdb cdb = new Cdb(Path.of(
                System.getProperty("java.io.tmpdir"),
                TEST_CDB_PREFIX + TEST_CDB_SUFFIX));
            cdb.close();
        });
    }

    /**
     * The cdb file is zero length. A {@link CdbException} is expected.
     */
    @Test
    public void testSadOpenEmptyFile() {
        assertThrows(CdbException.class, () -> {
            Path cdbFilePath = Files.createTempFile(TEST_CDB_PREFIX, TEST_CDB_SUFFIX);
            Cdb cdb = null;
            try {
                cdb = new Cdb(cdbFilePath);
            } finally {
                if (cdb != null) {
                    cdb.close();
                }
            }
        });
    }

    /**
     * Close the cdb twice.  This should be handled gracefully.
     * @throws IOException If the setup fails.
     */
    @Test
    public void testSadDoubleClose() throws IOException {
        Path cdbFilePath = createEmptyCdb();
        Cdb cdb = null;
        try {
            cdb = new Cdb(cdbFilePath);
            cdb.close();
            cdb.close();
        } finally {
            if (cdb != null) {
                cdb.close();
            }
        }
    }

    /**
     * Find a key/data for a simple cdb: one key/one value.
     * @throws IOException If the setup fails.
     */
    @Test
    public void testHappyFindSimple() throws IOException {
        Path cdbFilePath = createSimpleCdb();
        Cdb cdb = null;
        try {
            cdb = new Cdb(cdbFilePath);
            byte[] data = cdb.find(TEST_KEY_SINGLE);
            assertArrayEquals(TEST_DATA_SINGLE, data);
        } finally {
            if (cdb != null) {
                cdb.close();
            }
        }
    }

    /**
     * Find the first key/data for a more complex cdb. one key/two values.
     * @throws IOException If the setup fails.
     */
    @Test
    public void testHappyFindComplexFirst() throws IOException {
        Path cdbFilePath = createComplexCdb();
        Cdb cdb = null;
        try {
            cdb = new Cdb(cdbFilePath);
            byte[] actual = cdb.find(TEST_KEY_MULTI);
            // The actual may either of the values, so test for both.
            boolean contains = TEST_DATA_MULTI_LIST.stream()
                    .anyMatch(ba -> Arrays.equals(ba, actual));
            assertTrue(contains);
        } finally {
            if (cdb != null) {
                cdb.close();
            }
        }
    }

    /**
     * Find all of the key/data for a more complex cdb. one key/two values.
     * @throws IOException If the setup fails.
     */
    @Test
    public void testHappyFindComplexAll() throws IOException {
        Path cdbFilePath = createComplexCdb();
        Cdb cdb = null;
        try {
            cdb = new Cdb(cdbFilePath);
            final List<byte[]> actual = cdb.findAll(TEST_KEY_MULTI);
            assertMultiContains(actual);
        } finally {
            if (cdb != null) {
                cdb.close();
            }
        }
    }

    /**
     * Iterate over a key with a single value.
     * @throws IOException If the setup fails.
     */
    @Test
    public void testHappyIterateKeySimple() throws IOException {
        Path cdbFilePath = createSimpleCdb();
        Cdb cdb = null;
        try {
            cdb = new Cdb(cdbFilePath);
            Iterator<byte[]> cdbKeyIter = cdb.iterator(TEST_KEY_SINGLE);
            List<byte[]> actual = new ArrayList<>();
            cdbKeyIter.forEachRemaining(actual::add);
            assertEquals(1, actual.size());
            assertArrayEquals(TEST_DATA_SINGLE, actual.get(0));
        } finally {
            if (cdb != null) {
                cdb.close();
            }
        }
    }

    /**
     * Iterate over a key with a multiple values.
     * @throws IOException If the setup fails.
     */
    @Test
    public void testHappyIterateKeyComplex() throws IOException {
        Path cdbFilePath = createComplexCdb();
        Cdb cdb = null;
        try {
            cdb = new Cdb(cdbFilePath);
            Iterator<byte[]> cdbKeyIter = cdb.iterator(TEST_KEY_MULTI);
            List<byte[]> actual = new ArrayList<>();
            cdbKeyIter.forEachRemaining(actual::add);
            assertMultiContains(actual);
        } finally {
            if (cdb != null) {
                cdb.close();
            }
        }
    }

    /**
     * Iterate over a key with a multiple values.
     * @throws IOException If the setup fails.
     */
    @Test
    public void testHappyIterate() throws IOException {
        Path cdbFilePath = createComplexCdb();
        Cdb cdb = null;
        try {
            cdb = new Cdb(cdbFilePath);
            Iterator<ByteArrayPair> cdbKeyIter = cdb.iterator();
            List<byte[]> actual = new ArrayList<>();
            cdbKeyIter.forEachRemaining(bap -> {
                assertArrayEquals(TEST_KEY_MULTI, bap.first);
                actual.add(bap.second);
            });
            assertMultiContains(actual);
        } finally {
            if (cdb != null) {
                cdb.close();
            }
        }
    }

    private Path createEmptyCdb() throws IOException {
        Pair<Path, CdbBuilder> pair = openCdbMake();
        return finishCdbMake(pair);
    }

    private Path createSimpleCdb() throws IOException {
        Pair<Path, CdbBuilder> pair = openCdbMake();
        pair.second.add(TEST_KEY_SINGLE, TEST_DATA_SINGLE);
        return finishCdbMake(pair);
    }

    private Path createComplexCdb() throws IOException {
        Pair<Path, CdbBuilder> pair = openCdbMake();
        pair.second.add(TEST_KEY_MULTI, TEST_DATA_MULTI_ONE);
        pair.second.add(TEST_KEY_MULTI, TEST_DATA_MULTI_TWO);
        return finishCdbMake(pair);
    }

    private Pair<Path, CdbBuilder> openCdbMake() throws IOException {
        return new Pair<>(cdbFilePath.get(), new CdbBuilder(cdbFilePath.get()));
    }

    private Path finishCdbMake(Pair<Path, CdbBuilder> pair) {
        pair.second.build();
        return pair.first;
    }

    private void assertMultiContains(List<byte[]> dataList) {
        assertEquals(TEST_DATA_MULTI_LIST.size(), dataList.size());
        List<byte[]> containedData = new ArrayList<>(dataList.size());
        for (byte[] bb : TEST_DATA_MULTI_LIST) {
            boolean contained = false;
            for (byte[] ba : dataList) {
                if (Arrays.equals(ba, bb)) {
                    containedData.add(ba);
                    contained = true;
                }
            }
            assertTrue(contained);
        }
        assertEquals(TEST_DATA_MULTI_LIST.size(), containedData.size());
    }
}
