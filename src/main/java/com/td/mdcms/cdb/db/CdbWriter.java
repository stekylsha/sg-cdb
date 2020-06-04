/*
 * Copyright (C) 2019 by Teradata Corporation. All Rights Reserved. TERADATA CORPORATION
 * CONFIDENTIAL AND TRADE SECRET
 */
package com.td.mdcms.cdb.db;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.td.mdcms.cdb.exception.CdbException;
import com.td.mdcms.cdb.exception.CdbIOException;
import com.td.mdcms.cdb.internal.IntPair;
import com.td.mdcms.cdb.internal.Key;
import com.td.mdcms.cdb.model.ByteArrayPair;
import com.td.mdcms.cdb.model.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class CdbWriter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CdbWriter.class);

    /**
     * 256 entries * (Integer.BYTES * 2)
     */
    private static final int MAIN_TABLE_SIZE = 2048;

    /**
     * The max offset should be.  As the db is being built, if any offset is
     * greater than a 32-bit unsigned integer (4GB), it's a Bad Thing.
     */
	private static final long MAX_OFFSET = 0xffffffffL; // 2^32 - 1

    /**
     * Number of entries in the main hash table.
     */
    private static final int MAIN_TABLE_ENTRIES = 256;

    private final ByteBuffer intPairBuffer = ByteBuffer.allocate(Integer.BYTES * 2)
            .order(ByteOrder.LITTLE_ENDIAN);

    private RandomAccessFile cdbFile;
    private FileChannel cdbFileChannel;
    private final Path cdbPath;

    private Map<Integer, List<Pair<Key, Long>>> mainTableMap;

    /**
     * Constructs a CdbBuilder object and prepares it for the creation of a
     * constant database by manual manipulation.  No dump file is used.  The
     * tmp path is still necessary, as per spec.
     */
    public CdbWriter(Path cdbPath) throws CdbException {
        this.cdbPath = cdbPath;
        try {
            openCdbFile();
        } catch (IOException ex) {
            cleanUp();
            throw new CdbIOException("Could not open temp cdb file.", ex);
        }
        mainTableMap = new TreeMap<>();
    }

    public CdbWriter add(byte[] key, byte[] data) {
        return add(new ByteArrayPair(key, data));
    }

    public CdbWriter add(ByteArrayPair bap) {
        try {
            processCdbElement.accept(bap);
        } catch (CdbIOException ex) {
            cleanUp();
            throw ex;
        }
        return this;
    }

    public void close() throws CdbException {
        try {
            long slotTableStart = cdbFileChannel.position();

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
            cleanUp();
            throw new CdbIOException("Exception while writing cdb tmp file.", ex);
        } finally {
            closeFiles();
        }
    }

    private void openCdbFile() throws IOException {
        cdbFile = new RandomAccessFile(cdbPath.toFile(), "rw");
        cdbFileChannel = cdbFile.getChannel();
        cdbFileChannel.position(MAIN_TABLE_SIZE);
    }

    private void closeFiles() {
        if (cdbFileChannel != null) {
            try { cdbFileChannel.close(); } catch (IOException ex) {}
            cdbFileChannel = null;
        }
        if (cdbFile != null) {
            try { cdbFile.close(); } catch (IOException ex) {}
            cdbFile = null;
        }
    }

    private void cleanUp() {
        closeFiles();
        try { Files.deleteIfExists(cdbPath); } catch (IOException ex) {}
    }

    private final Consumer<ByteArrayPair> processCdbElement = (bap) -> {
        LOG.debug("Processing pair '{}', '{}'",
                new String(bap.first), new String(bap.second));
        Key key = new Key(bap.first);
        try {
            mainTableMap.computeIfAbsent(key.hashMod256(), k -> new ArrayList<>())
                    .add(new Pair<>(key, cdbFileChannel.position()));
            writeIntPair(bap.first.length, bap.second.length);
            cdbFileChannel.write(ByteBuffer.wrap(bap.first));
            cdbFileChannel.write(ByteBuffer.wrap(bap.second));
        } catch (IOException ex) {
            cleanUp();
            throw new CdbIOException("Could not write cdb element.", ex);
        }
    };

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
        cdbFileChannel.write(bb);
        return cdbFileChannel.position();
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

    private void positionIndex(int index) throws IOException {
        this.positionIndex(index, 0);
    }

    private void positionIndex(int index, long offset) throws IOException {
        cdbFileChannel.position(offset + (index << 3)); // effectively offset + (index * Integer.BYTES * 2)
    }

    private void writeIntPair(int first, int second) throws IOException {
        intPairBuffer.putInt(first);
        intPairBuffer.putInt(second);
        intPairBuffer.flip();
        cdbFileChannel.write(intPairBuffer);
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
}