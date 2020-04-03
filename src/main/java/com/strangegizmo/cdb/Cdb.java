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

/* Java imports. */

import java.io.DataInput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Cdb implements a Java interface to D.&nbsp;J.&nbsp;Bernstein's CDB
 * database.
 *
 * @author Michael Alyn Miller <malyn@strangeGizmo.com>
 * @version 1.0.3
 */
public class Cdb implements Iterable<CdbElement> {
    private static final long LONG_BYTE_MASK = 0x00ffL;
    private static final long LONG_INT_MASK = 0x00ffffffffL;
    private static final int INT_BYTE_MASK = 0x00ff;

    private static final int SLOT_TABLE_PAIRS = 256;

    /**
     * The RandomAccessFile for the CDB file.
     */
    private RandomAccessFile cdbFile;

    /**
     * The slot pointers, cached here for efficiency as we do not have
     * mmap() to do it for us.  These entries are paired as (pos, len)
     * tuples.
     */
    private int[] slotTable;

    /**
     * The number of hash slots searched under this key.
     */
    private int hashSlotIndex = 0;

    /**
     * The hash value for the current key.
     */
    private int keyHash = 0;

    /**
     * The number of hash slots in the hash table for the current key.
     */
    private int hashSlotCount = 0;

    /**
     * The position of the hash table for the current key
     */
    private int hashPosition = 0;

    /**
     * The position of the current key in the slot.
     */
    private int keyPosition = 0;

    /**
     * Creates an instance of the Cdb class and loads the given CDB
     * file.
     *
     * @param filepath The path to the CDB file to open.
     * @throws java.io.IOException if the CDB file could not be
     *                             opened.
     */
    public Cdb(String filepath) throws IOException {
        /* Open the CDB file. */
        cdbFile = new RandomAccessFile(filepath, "r");

        /* Read and parse the slot table.  We do not throw an exception
         * if this fails; the file might empty, which is not an error. */
        try {
            /* Create and parse the table. */
            slotTable = new int[SLOT_TABLE_PAIRS * 2];

            for (int i = 0; i < SLOT_TABLE_PAIRS; i++) {
                int pos = readInt(cdbFile);

                int len = readInt(cdbFile);

                slotTable[i << 1] = pos;
                slotTable[(i << 1) + 1] = len;
            }
        } catch (IOException ignored) {
            slotTable = null;
        }
    }

    /**
     * Closes the CDB database.
     */
    public final void close() {
        /* Close the CDB file. */
        try {
            cdbFile.close();
            cdbFile = null;
        } catch (IOException ignored) {
        }
    }

    /**
     * Finds the first record stored under the given key.
     *
     * @param key The key to search for.
     * @return The record store under the given key, or
     * <code>null</code> if no record with that key could be found.
     */
    public final synchronized byte[] find(byte[] key) {
        findinit();
        return findnext(key);
    }

    /**
     * Finds all the records for the given key.
     *
     * @param key The key to search for.
     * @return The record store under the given key, or
     * <code>null</code> if no record with that key could be found.
     */
    public final synchronized List<byte[]> findall(byte[] key) {
        findinit();
        List<byte[]> values = new ArrayList<>();
        byte[] value;
        while ((value = findnext(key)) != null) {
            values.add(value);
        }
        return values;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Iterator<CdbElement> iterator() {
        return new CdbIterator(cdbFile);
    }

    /**
     * Computes and returns the hash value for the given key.
     *
     * @param key The key to compute the hash value for.
     * @return The hash value of <code>key</code>.
     */
    static final int hash(byte[] key) {
        /* Initialize the hash value. */
        long h = 5381;

        /* Add each byte to the hash value. */
        for (byte b : key) {
            // h = ((h << 5) + h) ^ key[i];
            long k = (long)b & LONG_BYTE_MASK;
            h = (((h << 5) + h) ^ k) & LONG_INT_MASK;
        }

        /* Return the hash value. */
        return (int) (h & LONG_INT_MASK);
    }

    /**
     * Prepares the class to search for the given key.
     */
    private final void findinit() {
        hashSlotIndex = 0;
    }

    /**
     * Finds the next record stored under the given key.
     *
     * @param key The key to search for.
     * @return The next record store under the given key, or
     * <code>null</code> if no record with that key could be found.
     */
    private final synchronized byte[] findnext(byte[] key) {
        /* There are no keys if we could not read the slot table. */
		if (slotTable == null) {
			return null;
		}

        /* Locate the hash entry if we have not yet done so. */
        if (hashSlotIndex == 0) {
            /* Get the hash value for the key. */
            int u = hash(key);

            /* Unpack the information for this record. */
            int slot = u & 255;
            hashSlotCount = slotTable[(slot << 1) + 1];
			if (hashSlotCount == 0) {
				return null;
			}
            hashPosition = slotTable[slot << 1];

            /* Store the hash value. */
            keyHash = u;

            /* Locate the slot containing this key. */
            u >>>= 8;
            u %= hashSlotCount;
            u <<= 3;
            keyPosition = hashPosition + u;
        }

        /* Search all of the hash slots for this key. */
        try {
            while (hashSlotIndex < hashSlotCount) {
                /* Read the entry for this key from the hash slot. */
                cdbFile.seek(keyPosition);

                int h = readInt(cdbFile);
                int pos = readInt(cdbFile);
				if (pos == 0) {
					return null;
				}

                /* Advance the loop count and key position.  Wrap the
                 * key position around to the beginning of the hash slot
                 * if we are at the end of the table. */
                hashSlotIndex += 1;

                keyPosition += 8;
				if (keyPosition == (hashPosition + (hashSlotCount << 3))) {
					keyPosition = hashPosition;
				}

                /* Ignore this entry if the hash values do not match. */
				if (h != keyHash) {
					continue;
				}

                /* Get the length of the key and data in this hash slot
                 * entry. */
                cdbFile.seek(pos);

                int klen = readInt(cdbFile);
				if (klen != key.length) {
					continue;
				}

                int dlen = readInt(cdbFile);

                /* Read the key stored in this entry and compare it to
                 * the key we were given. */
                boolean match = true;
                byte[] k = new byte[klen];
                cdbFile.readFully(k);
                for (int i = 0; i < k.length; i++) {
                    if (k[i] != key[i]) {
                        match = false;
                        break;
                    }
                }

                /* No match; check the next slot. */
				if (!match) {
					continue;
				}

                /* The keys match, return the data. */
                byte[] d = new byte[dlen];
                cdbFile.readFully(d);
                return d;
            }
        } catch (IOException ignored) {
            return null;
        }

        /* No more data values for this key. */
        return null;
    }

    private int readInt(DataInput in) throws IOException {
        return (in.readUnsignedByte() & INT_BYTE_MASK)
                | ((in.readUnsignedByte() & INT_BYTE_MASK) << 8)
                | ((in.readUnsignedByte() & INT_BYTE_MASK) << 16)
                | ((in.readUnsignedByte() & INT_BYTE_MASK) << 24);
    }

    private class CdbIterator implements Iterator<CdbElement> {
        private final RandomAccessFile iteratorFile;
        private final int endOfData;
        private long lastOffset;

        public CdbIterator(RandomAccessFile raf) {
            try {
                iteratorFile = raf;
                iteratorFile.seek(0);
                int tmpEod = readInt(iteratorFile);
                /* Skip the rest of the hashtable. */
                iteratorFile.seek(2048);
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
                int keyLength = readInt(iteratorFile);
                int valueLength = readInt(iteratorFile);

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
}
