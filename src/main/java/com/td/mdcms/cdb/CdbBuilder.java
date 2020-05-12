/*
 * Copyright (C) 2019 by Teradata Corporation. All Rights Reserved. TERADATA CORPORATION
 * CONFIDENTIAL AND TRADE SECRET
 */
package com.td.mdcms.cdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.td.mdcms.cdb.exception.CdbException;
import com.td.mdcms.cdb.exception.CdbFormatException;
import com.td.mdcms.cdb.exception.CdbIOException;
import com.td.mdcms.cdb.internal.IntPair;
import com.td.mdcms.cdb.internal.Key;
import com.td.mdcms.cdb.model.Pair;

/**
 * Makes a constant database (cdb) as per the cdb spec.  From
 * https://cr.yp.to/cdb/cdb.txt:
 * <pre>
 *A cdb contains 256 pointers to linearly probed open hash tables. The
 * hash tables contain pointers to (key,data) pairs. A cdb is stored in
 * a single file on disk:
 *
 *     +----------------+---------+-------+-------+-----+---------+
 *     | p0 p1 ... p255 | records | hash0 | hash1 | ... | hash255 |
 *     +----------------+---------+-------+-------+-----+---------+
 * </pre>
 * There's more, but that's the gist of it.
 *
 * This class translates a dump file ...
 * <pre>
 *     A record is encoded for cdbmake as +klen,dlen:key->data followed by a
 *     newline. Here klen is the number of bytes in key and dlen is the number
 *     of bytes in data. The end of data is indicated by an extra newline.
 * </pre>
 * ... into a cdb file.
 *
 * Although it is more of a translator/bridge/adapter, it is named {@code Make}
 * for historical purposes.
 */
public final class CdbBuilder {

    /**
     * 256 entries * (Integer.BYTES * 2)
     */
    private static final int MAIN_TABLE_SIZE = 2048;

    /**
     * The default temp cdb file path, in case it is not explicit.
     */
    private static final Path DEFAULT_CDB_TMP_PATH =
            Path.of(System.getProperty("java.io.tmpdir"));

    /**
     * The default temp cdb file prefix, in case it is not explicit.
     */
    private static final String DEFAULT_CDB_TMP_PREFIX = "tmp-";

    /**
     * The max data length should be
     * <pre>
     *     <max integer> - <main table size> - <some slot table size>
     * </pre>
     *
     * We know main table size (2048), but have no idea the slot table size, and
     * the slot table entries <em>must</em> be within the 4GB limit.  As an
     * arbitrary limit, restrict the key and data values to 268435455 (24 bits).
     * This is smaller than the original code, but should be sufficient.
     */
	private static final int MAX_DATA_LENGTH = 0x0fffffff; // 268_435_455

    /**
     * Number of entries in the main hash table.
     */
    private static final int MAIN_TABLE_ENTRIES = 256;

    private final ByteBuffer intPairBuffer = ByteBuffer.allocate(Integer.BYTES * 2)
            .order(ByteOrder.LITTLE_ENDIAN);

    private RandomAccessFile cdbTmpFile;
    private FileChannel cdbTmpFileChannel;

    private final Path cdbPath;
    private final Path cdbTmpPath;
    private final Path cdbDumpPath;

    private Map<Integer, List<Pair<Key, Long>>> mainTableMap;

    /**
     * Constructs a CdbBuilder object and prepares it for the creation of a
     * constant database by manual manipulation.  No dump file is used.  The
     * tmp path is still necessary, as per spec.
     */
    public CdbBuilder(Path cdbPath) throws CdbException {
        this.cdbPath = cdbPath;
        this.cdbDumpPath = null;
        this.cdbTmpPath = DEFAULT_CDB_TMP_PATH.resolve(
                DEFAULT_CDB_TMP_PREFIX + cdbPath.getFileName());
        try {
            openCdbTmpFile();
            cdbTmpFileChannel.position(MAIN_TABLE_SIZE);
        } catch (IOException ex) {
            cleanUp();
            throw new CdbIOException("Could not open temp cdb file.", ex);
        }
        mainTableMap = new TreeMap<>();
    }

    /**
     * Constructs a CdbBuilder object and prepares it for the creation of a
     * constant database.  If the cdb file does not exist, create an empty one.
     * If it does exist, use it as the basis for creating a new one.
     */
    public CdbBuilder(Path cdbPath, Path cdbDumpPath) throws CdbException {
        this(cdbPath, cdbDumpPath,
                DEFAULT_CDB_TMP_PATH.resolve(DEFAULT_CDB_TMP_PREFIX + cdbPath.getFileName()));
    }

    /**
     * Constructs a CdbBuilder object and prepares it for the creation of a
     * constant database.  If the cdb file does not exist, create an empty one.
     * If it does exist, use it as the basis for creating a new one.
     */
    public CdbBuilder(Path cdbPath, Path cdbDumpPath, Path cdbTmpPath) throws CdbException {
        if (!Files.exists(cdbDumpPath)) {
            throw new CdbException("cdb dump file '" +
                    cdbDumpPath.toString() +
                    "' does not exist.");
        }
        this.cdbPath = cdbPath;
        this.cdbDumpPath = cdbDumpPath;
        this.cdbTmpPath = cdbTmpPath;
        mainTableMap = new TreeMap<>();
        try {
            openCdbTmpFile();
            processCdbDumpFile();
        } catch (IOException ex) {
            cleanUp();
            throw new CdbIOException("Could not open temp cdb file.", ex);
        }
    }

    public CdbBuilder add(byte[] key, byte[] data) {
        try {
            processCdbElement(new CdbElement(key, data));
        } catch (IOException ex) {
            cleanUp();
            throw new CdbIOException("Exception adding data.", ex);
        }
        return this;
    }

    public void build() throws CdbException {
        try {
            long slotTableStart = cdbTmpFileChannel.position();

            for (int i = 0 ; i < MAIN_TABLE_ENTRIES ; i++) {
                List<Pair<Key, Long>> mainList = mainTableMap.get(i);
                if (mainList != null) {
                    slotTableStart = writeElementMap(i, slotTableStart, mainList);
                } else {
                    positionIndex(i);
                    writeIntPair((int)slotTableStart, 0);
                }
            }
        } catch (IOException ex) {
            throw new CdbIOException("Exception while writing cdb tmp file.", ex);
        } finally {
            closeFiles();
        }

        try {
            // move the file to the real file.
            Files.move(cdbTmpPath, cdbPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new CdbIOException("Exception while writing cdb file.", ex);
        }
    }

    private void openCdbTmpFile() throws IOException {
        cdbTmpFile = new RandomAccessFile(cdbTmpPath.toFile(), "rw");
        cdbTmpFileChannel = cdbTmpFile.getChannel();
    }

    private void closeFiles() {
        if (cdbTmpFileChannel != null) {
            try { cdbTmpFileChannel.close(); } catch (IOException ex) {}
            cdbTmpFileChannel = null;
        }
        if (cdbTmpFile != null) {
            try { cdbTmpFile.close(); } catch (IOException ex) {}
            cdbTmpFile = null;
        }
    }

    private void cleanUp() {
        closeFiles();
        try { Files.deleteIfExists(cdbTmpPath); } catch (IOException ex) {}
    }

    private void processCdbDumpFile() {
        try (InputStream is = Files.newInputStream(cdbDumpPath, StandardOpenOption.READ)) {
            cdbTmpFileChannel.position(MAIN_TABLE_SIZE);
            processCdbDumpElements(is);
        } catch (IOException ex) {
            cleanUp();
            throw new CdbIOException("Exception reading cdb dump file.", ex);
        }
    }

    private void processCdbDumpElements(InputStream is)
            throws IOException {
        int chr;
        while ((chr = is.read()) != '\n') {
            if (chr != '+') {
                cleanUp();
                throw new CdbFormatException(
                        "Incorrect cdb dump file format.  " +
                                "Expected '+', read '" + (char) chr + "'.");
            }
            processCdbElement(readCdbElement(is));
        }
    }

    private void processCdbElement(CdbElement cdbElement)
                    throws IOException {
        mainTableMap.computeIfAbsent(cdbElement.key.hashMod256(), k -> new ArrayList<>())
                .add(new Pair<>(cdbElement.key, cdbTmpFileChannel.position()));
        writeIntPair(cdbElement.key.key.length, cdbElement.data.length);
        cdbTmpFileChannel.write(ByteBuffer.wrap(cdbElement.key.key));
        cdbTmpFileChannel.write(ByteBuffer.wrap(cdbElement.data));
    }

    private long writeElementMap(int mainTableIndex, long slotTableStart,
            List<Pair<Key, Long>> elementRefList) throws IOException {
        // write the main map entry
        positionIndex(mainTableIndex);
        // elementRefTableCapacity is the number of reference table entries
        int elementRefTableCapacity = elementRefList.size() << 1; // * 2
        writeIntPair((int) slotTableStart, elementRefTableCapacity);
        Map<Integer, List<IntPair>> elementRefMap =
                buildSlotMap(elementRefTableCapacity, elementRefList);
        int elementRefTableBytes = elementRefTableCapacity << 3;
        ByteBuffer bb = ByteBuffer.allocate(elementRefTableBytes)
                .order(ByteOrder.LITTLE_ENDIAN);
        IntBuffer elementRefTable = bb.asIntBuffer();
        elementRefMap.forEach((k, v) -> writeElementRef(elementRefTable,
                elementRefTableCapacity << 1,
                k << 1, v.iterator()));
        bb.position(elementRefTableBytes);
        bb.flip();
        positionIndex(0, slotTableStart);
        cdbTmpFileChannel.write(bb);
        return cdbTmpFileChannel.position();
    }

    private void writeElementRef(IntBuffer elementRefTable,
            int elementRefTableCount, int elementIndex,
            Iterator<IntPair> elementRefIter) {
        IntPair elementRef = elementRefIter.next();
        elementRefTable.put(elementIndex++, elementRef.first);
        elementRefTable.put(elementIndex++, elementRef.second);
        if (elementRefIter.hasNext()) {
            if (elementIndex >= elementRefTableCount) {
                elementIndex = 0;
            }
            writeElementRef(elementRefTable, elementRefTableCount,
                    elementIndex, elementRefIter);
        }
    }

    private CdbElement readCdbElement(InputStream is) throws IOException {
        IntPair lengths = new IntPair(
                readIntFromStream(is, ','),
                readIntFromStream(is, ':'));
        if (lengths.first > MAX_DATA_LENGTH || lengths.second > MAX_DATA_LENGTH) {
            cleanUp();
            throw new CdbFormatException("Key or data is too large for cdb.");
        }
        return new CdbElement(
                readBytesFromStream(is, lengths.first, "->".getBytes()),
                readBytesFromStream(is, lengths.second, "\n".getBytes())
        );
    }

    private int readIntFromStream(InputStream is, char terminator)
            throws CdbFormatException, IOException {
        int intRead = 0;
        int chr;
        while ((chr = is.read()) >= 0 && chr != terminator) {
            if (Character.isDigit(chr)) {
                intRead = (intRead * 10) + Character.getNumericValue(chr);
            }
        }
        if (chr != terminator) {
            cleanUp();
            throw new CdbFormatException(
                    "Incorrect cdb dump file format.  " +
                            "Expected '" + terminator +
                            "', read '" + (char)chr + "'.");
        }
        return intRead;
    }

    private byte[] readBytesFromStream(InputStream is, int count, byte[] terminator)
            throws IOException {
        byte[] ba = new byte[count];
        int actual = is.read(ba);
        if (actual != count) {
            throw new CdbFormatException("Expected " + count + " bytes but read " + actual);
        }
        byte[] trm = new byte[terminator.length];
        if (!(is.read(trm) == terminator.length && Arrays.equals(terminator, trm))) {
            cleanUp();
            throw new CdbFormatException(
                    "Incorrect cdb dump file format.  " +
                            "Expected '" + new String(terminator) +
                            "', read '" + new String(trm) + "'.");
        }
        return ba;
    }

    private void positionIndex(int index) throws IOException {
        this.positionIndex(index, 0);
    }

    private void positionIndex(int index, long offset) throws IOException {
        cdbTmpFileChannel.position(offset + (index << 3)); // effectively offset + (index * Integer.BYTES * 2)
    }

    private void writeIntPair(int first, int second) throws IOException {
        intPairBuffer.putInt(first);
        intPairBuffer.putInt(second);
        intPairBuffer.flip();
        cdbTmpFileChannel.write(intPairBuffer);
        intPairBuffer.clear();
    }

    private Map<Integer, List<IntPair>> buildSlotMap(
            int slotTableSize, List<Pair<Key, Long>> slotInfoList) {
        return slotInfoList.stream()
                .collect(Collectors.groupingBy(
                        p -> p.first.hashDiv256() % slotTableSize,
                        TreeMap::new,
                        Collectors.mapping(
                                p -> new IntPair(p.first.hash, p.second.intValue()),
                                Collectors.toList()
                        )
                ));
    }

    private static class CdbElement {
        /** The key value for this element. */
        public final Key key;

        /** The data value for this element. */
        public final byte[] data;

        /**
         * Creates an instance of the CdbElement class and initializes it
         * with the given key and data values.
         *
         * @param keyBytes The key value for this element.
         * @param data The data value for this element.
         */
        public CdbElement(byte[] keyBytes, byte[] data) {
            this.key = new Key(keyBytes);
            this.data = data;
        }
    }
}