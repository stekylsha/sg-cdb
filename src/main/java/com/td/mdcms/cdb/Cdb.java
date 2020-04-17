/*
 * Copyright (C) 2019 by Teradata Corporation. All Rights Reserved. TERADATA CORPORATION
 * CONFIDENTIAL AND TRADE SECRET
 */
package com.td.mdcms.cdb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.td.mdcms.cdb.exception.CdbException;
import com.td.mdcms.cdb.exception.CdbFormatException;
import com.td.mdcms.cdb.exception.CdbIOException;
import com.td.mdcms.cdb.exception.CdbStateException;
import com.td.mdcms.cdb.internal.CdbElement;
import com.td.mdcms.cdb.internal.IntPair;
import com.td.mdcms.cdb.internal.Key;
import com.td.mdcms.cdb.internal.SlotTableInfo;
import com.td.mdcms.cdb.model.ByteArrayPair;

/**
 * Cdb implements a Java interface to D.&nbsp;J.&nbsp;Bernstein's CDB
 * database.
 *
 * @author Michael Alyn Miller <malyn@strangeGizmo.com>
 * @version 1.0.3
 */
public class Cdb implements Iterable<ByteArrayPair> {

    /**
     * 256 entries * (4 bytes + 4 bytes)
     */
    private static final int MAIN_TABLE_SIZE = 2048;

    /**
     * Expected read size (Two integers)
     */
    private static final int CDB_READ_SIZE = Integer.BYTES * 2;

    /**
     * The RandomAccessFile for the CDB file.
     */
    private RandomAccessFile cdbFile;

    /**
     * The FileChannel for the CDB file.
     */
    private FileChannel cdbFileChannel;

    /**
     * The main pointers. These entries are paired as sub-table index/number
     * of entries.
     */
    private IntBuffer mainTable;

    /**
     * Common buffer for reading two integers.
     */
    private final ByteBuffer intPairBuffer = ByteBuffer.allocate(Integer.BYTES * 2)
            .order(ByteOrder.LITTLE_ENDIAN);

    /**
     * Creates an instance of the Cdb class and loads the given CDB
     * file.
     *
     * @param filepath The path to the CDB file to open.
     * @throws {@link CdbException} if the CDB file could not be
     * opened.
     */
    public Cdb(Path filepath) throws CdbException {
        /* Open the CDB file. */
        try {
            cdbFile = new RandomAccessFile(filepath.toFile(), "r");
            cdbFileChannel = cdbFile.getChannel();
            mainTable = cdbFileChannel
                    .map(FileChannel.MapMode.READ_ONLY, 0, MAIN_TABLE_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .asIntBuffer();
        } catch (IOException ex) {
            throw new CdbException("Could not open the cdb file.", ex);
        }
    }

    /**
     * Closes the CDB database.
     */
    public final void close() {
        /* Close the CDB file. */
        try {
            if (cdbFile != null) {
                cdbFile.close();
                cdbFile = null;
                cdbFileChannel = null;
                mainTable = null;
            }
        } catch (IOException ignored) {
            // Not really anything to do here.
        }
    }

    /**
     * Finds the first record stored under the given key.
     *
     * @param byteKey The key to search for.
     * @return The record store under the given key, or
     * {@code null} if no record with that key could be found.
     */
    public final byte[] find(byte[] byteKey) {
        Key key = new Key(byteKey);
        List<Long> recordOffsets;
        try {
            recordOffsets = initFind(key);
        } catch (IOException ex) {
            // FIXME change it into a CdbException
            throw new CdbIOException("Could not get the record offsets.", ex);
        }
        byte[] record = null;
        if (!recordOffsets.isEmpty()) {
            record = readRecord(key, recordOffsets.get(0));
        }
        return record;
    }

    /**
     * Finds all the records for the given key.
     *
     * @param byteKey The key to search for.
     * @return The records stored under the given key, or
     * an empty list if no records with that key could be found.
     */
    public final List<byte[]> findAll(byte[] byteKey) {
        final Key key = new Key(byteKey);
        List<Long> recordOffsets;
        try {
            recordOffsets = initFind(key);
        } catch (IOException ex) {
            throw new CdbIOException("Could not get the record offsets.", ex);
        }
        return recordOffsets.stream()
                .map(ro -> readRecord(key, ro))
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Finds the records for the given key as an iterator.  This is a lot like
     * the spec's cdb_findstart/cdb_find, but as an iterator.
     *
     * @param byteKey The key to search for.
     * @return An iterator for the records for the key.
     */
    public Iterator<byte[]> iterator(byte[] byteKey) {
        return new KeyIterator(new Key(byteKey));
    }

    /**
     * @inheritDoc
     */
    @Override
    public Iterator<ByteArrayPair> iterator() {
        return new CdbIterator();
    }

    private List<Long> initFind(Key key) throws IOException {
        SlotTableInfo slotTableInfo = new SlotTableInfo(mainTable, key);
        if (slotTableInfo.hasEntries()) {
            return getRecordOffsets(slotTableInfo);
        }
        return Collections.emptyList();
    }

    private List<Long> getRecordOffsets(SlotTableInfo slotTableInfo)
            throws IOException {
        List<Long> offsets = new ArrayList<>();
        int keyHash = slotTableInfo.getKey().hash;
        long subtableOffset = slotTableInfo.offset();
        long entry = slotTableInfo.firstEntry();
        synchronized (cdbFile) {
            cdbFile.seek(subtableOffset + (entry << 3));
            boolean done = false;
            while (!done) {
                // pair.first -> hash, pair.second -> record offset
                IntPair pair = readIntPair(cdbFileChannel);
                if (pair.first == 0 || pair.second == 0) {
                    done = true;
                } else if (pair.first == keyHash) {
                    offsets.add(Integer.toUnsignedLong(pair.second));
                }
                if (!done && ++entry >= slotTableInfo.entries()) {
                    entry = 0;
                    cdbFile.seek(subtableOffset);
                }
            }
        }
        return offsets;
    }

    private byte[] readRecord(Key key, long recordOffset) {
        byte[] record = null;
        try {
            synchronized (cdbFile) {
                cdbFile.seek(recordOffset);
                // read key and data length
                // pair.first -> key length, pair.second -> data length
                IntPair pair = readIntPair(cdbFileChannel);
                if (pair.first == key.key.length) {
                    ByteBuffer bb = ByteBuffer.allocate(pair.first);
                    if (cdbFileChannel.read(bb) != pair.first) {
                        throw new CdbFormatException("CDB record key read issue");
                    }
                    if (Arrays.equals(key.key, bb.array())) {
                        bb.clear();
                        record = new byte[pair.second];
                        bb = ByteBuffer.wrap(record);
                        if (cdbFileChannel.read(bb) != pair.second) {
                            throw new CdbFormatException("CDB record data read issue");
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new CdbIOException("Exception while reading record", ex);
        }
        return record;
    }

    private IntPair readIntPair(FileChannel channel) throws IOException {
        IntPair pair = null;
        synchronized (intPairBuffer) {
            if (CDB_READ_SIZE != channel.read(intPairBuffer)) {
                throw new CdbFormatException("CDB int pair read issue");
            }
            IntBuffer ib = intPairBuffer.flip().asIntBuffer();
            pair = new IntPair(ib.get(), ib.get());
            intPairBuffer.clear();
        }
        return pair;
    }

    private class KeyIterator implements Iterator<byte[]> {
        private final Key key;
        private final Iterator<Long> recordOffsetIterator;
        private byte[] nextRecord;

        public KeyIterator(Key key) {
            try {
                this.key = key;
                recordOffsetIterator = initFind(key).iterator();
                if (recordOffsetIterator.hasNext()) {
                    nextRecord = readRecord(key, recordOffsetIterator.next());
                }
            } catch (IOException ex) {
                throw new CdbIOException("Problem creating key iterator for '" +
                        new String(key.key) + "'", ex);
            }
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        @Override
        public byte[] next() {
            byte[] tmpNextRecord = nextRecord;
            if (nextRecord == null) {
                throw new CdbStateException("KeyIterator has no more records.");
            } else if (!recordOffsetIterator.hasNext()) {
                nextRecord = null;
            } else {
                nextRecord = readRecord(key, recordOffsetIterator.next());
            }
            return tmpNextRecord;
        }
    }

    private class CdbIterator implements Iterator<ByteArrayPair> {
        private final int endOfData;
        private long lastOffset;

        public CdbIterator() {
            try {
                synchronized (cdbFile) {
                    cdbFile.seek(0);
                    IntPair tmpEod = readIntPair(cdbFileChannel);
                    /* Skip the rest of the hashtable. */
                    cdbFile.seek(MAIN_TABLE_SIZE);
                    lastOffset = cdbFile.getFilePointer();
                    endOfData = tmpEod.first;
                }
            } catch (IOException ioe) {
                throw new CdbException("Problem creating cdb iterator");
            }
        }

        @Override
        public boolean hasNext() {
            return lastOffset < endOfData;
        }

        @Override
        public ByteArrayPair next() {
            try {
                synchronized (cdbFile) {
                    // make sure to pick up where we left off
                    if (lastOffset != cdbFile.getFilePointer()) {
                        cdbFile.seek(lastOffset);
                    }

                    // read key/value lengths
                    // pair.first -> key length, pair.second -> data length
                    IntPair lengths = readIntPair(cdbFileChannel);

                    // read the record
                    ByteBuffer[] record = new ByteBuffer[] {
                            ByteBuffer.wrap(new byte[lengths.first]),
                            ByteBuffer.wrap(new byte[lengths.second])
                    };
                    if (cdbFileChannel.read(record) != (lengths.first + lengths.second)) {
                        throw new IOException("unrecognizable cdb format");
                    }

                    lastOffset = cdbFile.getFilePointer();
                    return new ByteArrayPair(record[0].array(), record[1].array());
                }
            } catch (IOException ioe) {
                throw new CdbException("Iterator next failure", ioe);
            }
        }
    }
}
