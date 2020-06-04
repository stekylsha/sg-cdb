package com.td.mdcms.cdb.db;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static com.td.mdcms.cdb.TestResources.*;

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

public class CdbReaderTest {

    private static final List<byte[]> TEST_DATA_MULTI_LIST =
            List.of(HAPPY_MULTI_DATA_ONE, HAPPY_MULTI_DATA_TWO);

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
        try (CdbReader cdb = new CdbReader(cdbPath)) {
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
            CdbReader cdb = new CdbReader(Path.of(
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
            try (CdbReader cdb = new CdbReader(cdbEmptyFilePath)) {
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
        try (CdbReader cdb = new CdbReader(cdbPath)) {
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
        try (CdbReader cdb = new CdbReader(cdbPath)) {
            byte[] data = cdb.readOne(HAPPY_SIMPLE_KEY);
            assertArrayEquals(HAPPY_SIMPLE_DATA, data);
        }
    }

    /**
     * Find the first key/data for a more complex cdb. one key/two values.
     */
    @Test
    public void testHappyFindComplexFirst() {
        Path cdbPath = createComplexCdb();
        try (CdbReader cdb = new CdbReader(cdbPath)) {
            byte[] actual = cdb.readOne(HAPPY_MULTI_KEY);
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
        try (CdbReader cdb = new CdbReader(cdbPath)) {
            final List<byte[]> actual = cdb.readAll(HAPPY_MULTI_KEY);
            assertMultiContains(actual);
        }
    }

    /**
     * Iterate over a key with a single value.
     */
    @Test
    public void testHappyIterateKeySimple() {
        Path cdbPath = createSimpleCdb();
        try (CdbReader cdb = new CdbReader(cdbPath)) {
            Iterator<byte[]> cdbKeyIter = cdb.iterator(HAPPY_SIMPLE_KEY);
            List<byte[]> actual = new ArrayList<>();
            cdbKeyIter.forEachRemaining(actual::add);
            assertEquals(1, actual.size());
            assertArrayEquals(HAPPY_SIMPLE_DATA, actual.get(0));
        }
    }

    /**
     * Iterate over a key with a multiple values.
     */
    @Test
    public void testHappyIterateKeyComplex() {
        Path cdbPath = createComplexCdb();
        try (CdbReader cdb = new CdbReader(cdbPath)) {
            Iterator<byte[]> cdbKeyIter = cdb.iterator(HAPPY_MULTI_KEY);
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
        try (CdbReader cdb = new CdbReader(cdbPath)) {
            Iterator<ByteArrayPair> cdbKeyIter = cdb.iterator();
            List<byte[]> actual = new ArrayList<>();
            cdbKeyIter.forEachRemaining(bap -> {
                assertArrayEquals(HAPPY_MULTI_KEY, bap.first);
                actual.add(bap.second);
            });
            assertMultiContains(actual);
        }
    }

    private Path createEmptyCdb() {
        Pair<Path, CdbWriter> pair = openCdb();
        return finishCdbMake(pair);
    }

    private Path createSimpleCdb() {
        Pair<Path, CdbWriter> pair = openCdb();
        pair.second.add(HAPPY_SIMPLE_KEY, HAPPY_SIMPLE_DATA);
        return finishCdbMake(pair);
    }

    private Path createComplexCdb() {
        Pair<Path, CdbWriter> pair = openCdb();
        pair.second.add(HAPPY_MULTI_KEY, HAPPY_MULTI_DATA_ONE);
        pair.second.add(HAPPY_MULTI_KEY, HAPPY_MULTI_DATA_TWO);
        return finishCdbMake(pair);
    }

    private Pair<Path, CdbWriter> openCdb() {
        return new Pair<>(cdbFilePath.get(), new CdbWriter(cdbFilePath.get()));
    }

    private Path finishCdbMake(Pair<Path, CdbWriter> pair) {
        pair.second.close();
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
