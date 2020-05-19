package com.td.mdcms.cdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import org.junit.jupiter.api.Disabled;
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
     */
    @Test
    public void testHappyOpenClose() {
        Path cdbPath = createEmptyCdb();
        try (Cdb cdb = new Cdb(cdbPath)) {
            // Just checking to make sure it works.
            assertNotNull(cdb);
        }
    }

    /**
     * The cdb file does not exist. A {@link CdbException} is expected.
     */
    @Test
    public void testSadOpenNoFile() {
        assertThrows(CdbException.class, () -> {
            Cdb cdb = new Cdb(Path.of(
                System.getProperty("java.io.tmpdir"),
                TEST_CDB_PREFIX + TEST_CDB_SUFFIX));
            cdb.close();
        });
    }

    /**
     * The cdb file is zero length. A {@link CdbException} is expected.
     * @throws IOException Maybe, when creating or deleting the temp file.
     */
    @Test
    public void testSadOpenEmptyFile() throws IOException {
        Path cdbEmptyFilePath = Files.createTempFile(TEST_CDB_PREFIX, TEST_CDB_SUFFIX);
        assertThrows(CdbException.class, () -> {
            try (Cdb cdb = new Cdb(cdbEmptyFilePath)) {
                // should never be here
                fail("Badness ensues.");
            }
        });
        Files.deleteIfExists(cdbEmptyFilePath);
    }

    /**
     * Close the cdb twice.  This should be handled gracefully.
     */
    @Test
    public void testSadDoubleClose() {
        Path cdbPath = createEmptyCdb();
        try (Cdb cdb = new Cdb(cdbPath)) {
            cdb.close();
            cdb.close();
        }
    }

    /**
     * Find a key/data for a simple cdb: one key/one value.
     */
    @Test
    public void testHappyFindSimple() {
        Path cdbPath = createSimpleCdb();
        try (Cdb cdb = new Cdb(cdbPath)) {
            byte[] data = cdb.find(TEST_KEY_SINGLE);
            assertArrayEquals(TEST_DATA_SINGLE, data);
        }
    }

    /**
     * Find the first key/data for a more complex cdb. one key/two values.
     */
    @Test
    public void testHappyFindComplexFirst() {
        Path cdbPath = createComplexCdb();
        try (Cdb cdb = new Cdb(cdbPath)) {
            byte[] actual = cdb.find(TEST_KEY_MULTI);
            // The actual may be either of the values, so test for both.
            boolean contains = TEST_DATA_MULTI_LIST.stream()
                    .anyMatch(ba -> Arrays.equals(ba, actual));
            assertTrue(contains);
        }
    }

    /**
     * Find all of the key/data for a more complex cdb. one key/two values.
     */
    @Test
    public void testHappyFindComplexAll() {
        Path cdbPath = createComplexCdb();
        try (Cdb cdb = new Cdb(cdbPath)) {
            final List<byte[]> actual = cdb.findAll(TEST_KEY_MULTI);
            assertMultiContains(actual);
        }
    }

    /**
     * Iterate over a key with a single value.
     */
    @Test
    public void testHappyIterateKeySimple() {
        Path cdbPath = createSimpleCdb();
        try (Cdb cdb = new Cdb(cdbPath)) {
            Iterator<byte[]> cdbKeyIter = cdb.iterator(TEST_KEY_SINGLE);
            List<byte[]> actual = new ArrayList<>();
            cdbKeyIter.forEachRemaining(actual::add);
            assertEquals(1, actual.size());
            assertArrayEquals(TEST_DATA_SINGLE, actual.get(0));
        }
    }

    /**
     * Iterate over a key with a multiple values.
     */
    @Test
    public void testHappyIterateKeyComplex() {
        Path cdbPath = createComplexCdb();
        try (Cdb cdb = new Cdb(cdbPath)) {
            Iterator<byte[]> cdbKeyIter = cdb.iterator(TEST_KEY_MULTI);
            List<byte[]> actual = new ArrayList<>();
            cdbKeyIter.forEachRemaining(actual::add);
            assertMultiContains(actual);
        }
    }

    /**
     * Iterate over a key with a multiple values.
     */
    @Test
    public void testHappyIterate() {
        Path cdbPath = createComplexCdb();
        try (Cdb cdb = new Cdb(cdbPath)) {
            Iterator<ByteArrayPair> cdbKeyIter = cdb.iterator();
            List<byte[]> actual = new ArrayList<>();
            cdbKeyIter.forEachRemaining(bap -> {
                assertArrayEquals(TEST_KEY_MULTI, bap.first);
                actual.add(bap.second);
            });
            assertMultiContains(actual);
        }
    }

    private Path createEmptyCdb() {
        Pair<Path, CdbBuilder> pair = openCdbMake();
        return finishCdbMake(pair);
    }

    private Path createSimpleCdb() {
        Pair<Path, CdbBuilder> pair = openCdbMake();
        pair.second.add(TEST_KEY_SINGLE, TEST_DATA_SINGLE);
        return finishCdbMake(pair);
    }

    private Path createComplexCdb() {
        Pair<Path, CdbBuilder> pair = openCdbMake();
        pair.second.add(TEST_KEY_MULTI, TEST_DATA_MULTI_ONE);
        pair.second.add(TEST_KEY_MULTI, TEST_DATA_MULTI_TWO);
        return finishCdbMake(pair);
    }

    private Pair<Path, CdbBuilder> openCdbMake() {
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
