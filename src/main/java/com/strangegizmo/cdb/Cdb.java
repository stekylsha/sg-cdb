/*
 * Copyright (c) 2000-2001, Michael Alyn Miller <malyn@strangeGizmo.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of Michael Alyn Miller nor the names of the
 *    contributors to this software may be used to endorse or promote
 *    products derived from this software without specific prior written
 *    permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package com.strangegizmo.cdb;

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

/**
 * Cdb implements a Java interface to D.&nbsp;J.&nbsp;Bernstein's CDB
 * database.
 *
 * @author Michael Alyn Miller <malyn@strangeGizmo.com>
 * @version 1.0.3
 */
public class Cdb implements Iterable<CdbElement> {

    /**
     * 256 entries * (4 bytes + 4 bytes)
     */
    private static final int SLOT_TABLE_SIZE = 2048;

    /**
     * Expected read size (Two integers)
     */
    private static final int CDB_READ_SIZE = Integer.BYTES * 2;

    /**
     * Initial hash value
     */
    private static final int INITIAL_HASH = 5381;

    /**
     * The RandomAccessFile for the CDB file.
     */
    private RandomAccessFile cdbFile;

    /**
     * The main pointers. These entries are paired as sub-table index/number
     * of entries.
     */
    private IntBuffer mainTable;

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
            mainTable = cdbFile.getChannel()
                    .map(FileChannel.MapMode.READ_ONLY, 0, SLOT_TABLE_SIZE)
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
                mainTable = null;
            }
        } catch (IOException ignored) {
        }
    }

    /**
     * Finds the first record stored under the given key.
     *
     * @param byteKey The key to search for.
     * @return The record store under the given key, or
     * {@code null} if no record with that key could be found.
     */
    public final synchronized byte[] find(byte[] byteKey) {
        Key key = new Key(byteKey);
        List<Integer> recordOffsets;
        try {
            recordOffsets = initFind(key);
        } catch (IOException ex) {
            // FIXME change it into a CdbException
            throw new CdbException("Could not get the record offsets.", ex);
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
    public final synchronized List<byte[]> findAll(byte[] byteKey) {
        final Key key = new Key(byteKey);
        List<Integer> recordOffsets;
        try {
            recordOffsets = initFind(key);
        } catch (IOException ex) {
            throw new CdbException("Could not get the record offsets.", ex);
        }
        return recordOffsets.stream()
                .map(ro -> readRecord(key, ro))
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * @inheritDoc
     */
    @Override
    public Iterator<CdbElement> iterator() {
        return new CdbIterator(cdbFile);
    }

    private List<Integer> initFind(Key key) throws IOException {
        SubtableInfo subtableInfo = new SubtableInfo(key);
        if (subtableInfo.hasEntries()) {
            return getRecordOffsets(subtableInfo);
        }
        return Collections.emptyList();
    }

    private List<Integer> getRecordOffsets(SubtableInfo subtableInfo)
            throws IOException {
        List<Integer> offsets = new ArrayList<>();
        ByteBuffer bb = ByteBuffer.allocate(CDB_READ_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        int keyHash = subtableInfo.getKey().getHash();
        int subtableOffset = subtableInfo.getOffset();
        int entry = subtableInfo.getFirstEntry();
        cdbFile.seek(subtableOffset + (entry << 3));
        FileChannel cdbChannel = cdbFile.getChannel();
        boolean done = false;
        while (!done) {
            if (cdbChannel.read(bb) != CDB_READ_SIZE) {
                throw new CdbException("CDB record read issue");
            }
            IntBuffer ib = bb.flip().asIntBuffer();
            int hash = ib.get();
            int record = ib.get();
            if (hash == 0 || record == 0) {
                done = true;
            } else if (hash == keyHash) {
               offsets.add(record);
            }
            if (!done && ++entry >= subtableInfo.getEntries()) {
                entry = 0;
                cdbFile.seek(subtableOffset);
            }
            bb.clear();
        }
        return offsets;
    }

    private byte[] readRecord(Key key, int recordOffset) {
        byte[] record = null;
        try {
            cdbFile.seek(recordOffset);
            // read key and data length
            ByteBuffer bb = ByteBuffer.allocate(CDB_READ_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN);
            FileChannel cdbChannel = cdbFile.getChannel();
            if (CDB_READ_SIZE != cdbChannel.read(bb)) {
                throw new CdbException("CDB record size read issue");
            }
            IntBuffer ib = bb.flip().asIntBuffer();
            int keyLength = ib.get();
            int dataLength = ib.get();
            if (keyLength == key.getKey().length) {
                bb.clear();
                bb = ByteBuffer.allocate(keyLength);
                if (keyLength != cdbChannel.read(bb)) {
                    throw new CdbException("CDB record key read issue");
                }
                if (Arrays.equals(key.getKey(), bb.array())) {
                    bb.clear();
                    record = new byte[dataLength];
                    bb = ByteBuffer.wrap(record);
                    if (dataLength != cdbChannel.read(bb)) {
                        throw new CdbException("CDB record data read issue");
                    }
                }
            }
        } catch (IOException ex) {
            throw new CdbException("Exception while reading record", ex);
        }
        return record;
    }

    class Pair<S, T> {
        public final S first;
        public final T second;

        public Pair(S first, T second) {
            this.first = first;
            this.second = second;
        }
    }

    private class IntPair extends Pair<Integer, Integer> {
        public IntPair(Integer first, Integer second) {
            super(first, second);
        }
    }

    static class Key {
        private final byte[] key;
        private final int hash;

        public Key(byte[] key) {
            this.key = key;
            this.hash = hash(key);
            int tmpHash = CdbMake.hash(key);
        }

        public byte[] getKey() {
            return Arrays.copyOf(key, key.length);
        }

        public int getHash() {
            return hash;
        }

        /**
         * Computes and returns the hash value for the given key.  Hash function
         * as defined by the spec is {@code h = ((h << 5) + h) ^ c}.
         *
         * @param key The key to compute the hash value for.
         * @return The hash value of {@code key}.
         */
        static int hash(byte[] key) {
            int h = INITIAL_HASH;
            for (byte b : key) {
                int k = Byte.toUnsignedInt(b);
                h = ((h << 5) + h) ^ k;
            }
            return h;
        }
    }

    private class SubtableInfo {
        private final Key key;
        private final IntPair offsetEntries;

        public SubtableInfo(Key key) {
            this.key = key;

            // Get sub table info
            int tableSlot = (key.getHash() & 0x00ff) << 1; // (h % 256) * 2 entries per slot
            int[] subTableInfo = new int[2]; // subtable index +  number of entries
            mainTable.position(tableSlot)
                    .duplicate()
                    .get(subTableInfo);
            this.offsetEntries = new IntPair(subTableInfo[0], subTableInfo[1]);
        }

        public Key getKey() {
            return key;
        }

        public int getOffset() {
            return offsetEntries.first;
        }

        public int getEntries() {
            return offsetEntries.second;
        }

        public int getFirstEntry() {
            if (!hasEntries()) {
                throw new IllegalStateException("No entries exist for key '" +
                        new String(key.getKey()) + "'");
            }
            return (key.getHash() >>> 8) % offsetEntries.second;
        }

        public boolean hasEntries() {
            return offsetEntries.second != 0;
        }
    }

    private class CdbIterator implements Iterator<CdbElement> {
        private final RandomAccessFile iteratorFile;
        private final FileChannel iteratorChannel;
        private final int endOfData;
        private long lastOffset;

        public CdbIterator(RandomAccessFile raf) {
            try {
                iteratorFile = raf;
                iteratorChannel = raf.getChannel();
                iteratorFile.seek(0);
                int tmpEod = readInt(iteratorChannel);
                /* Skip the rest of the hashtable. */
                iteratorFile.seek(SLOT_TABLE_SIZE);
                lastOffset = iteratorFile.getFilePointer();
                endOfData = tmpEod;
            } catch (IOException ioe) {
                throw new CdbException("Iterator creation failure", ioe);
            }
        }

        @Override
        public boolean hasNext() {
            return lastOffset < endOfData;
        }

        @Override
        public CdbElement next() {
            try {
                // make sure to pick up where we left off
                if (lastOffset != iteratorFile.getFilePointer()) {
                    iteratorFile.seek(lastOffset);
                }

                // read key/value lengths
                int keyLength = readInt(iteratorChannel);
                int valueLength = readInt(iteratorChannel);

                // read the key
                byte[] key = new byte[keyLength];
                if (iteratorFile.read(key) != keyLength) {
                    throw new IOException("unrecognizable cdb format");
                }
                // read the value
                byte[] value = new byte[valueLength];
                if (iteratorFile.read(value) != valueLength) {
                    throw new IOException("unrecognizable cdb format");
                }

                lastOffset = iteratorFile.getFilePointer();
                return new CdbElement(key, value);
            } catch (IOException ioe) {
                throw new CdbException("Iterator next failure", ioe);
            }
        }
    }

    private int readInt(FileChannel fc) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        if (fc.read(bb) != Integer.BYTES) {
            throw new IOException("Unable to read integer.");
        }
        bb.flip();
        return bb.asIntBuffer().get();
    }
}
