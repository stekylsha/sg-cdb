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
    private RandomAccessFile file_;

    /**
     * The slot pointers, cached here for efficiency as we do not have
     * mmap() to do it for us.  These entries are paired as (pos, len)
     * tuples.
     */
    private int[] slotTable_;

    /**
     * The number of hash slots searched under this key.
     */
    private int loop_ = 0;

    /**
     * The hash value for the current key.
     */
    private int khash_ = 0;

    /**
     * The number of hash slots in the hash table for the current key.
     */
    private int hslots_ = 0;

    /**
     * The position of the hash table for the current key
     */
    private int hpos_ = 0;

    /**
     * The position of the current key in the slot.
     */
    private int kpos_ = 0;

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
        file_ = new RandomAccessFile(filepath, "r");

        /* Read and parse the slot table.  We do not throw an exception
         * if this fails; the file might empty, which is not an error. */
        try {
            /* Create and parse the table. */
            slotTable_ = new int[SLOT_TABLE_PAIRS * 2];

            int offset = 0;
            for (int i = 0; i < SLOT_TABLE_PAIRS; i++) {
                int pos = readInt(file_);

                int len = readInt(file_);

                slotTable_[i << 1] = pos;
                slotTable_[(i << 1) + 1] = len;
            }
        } catch (IOException ignored) {
            slotTable_ = null;
        }
    }

    /**
     * Closes the CDB database.
     */
    public final void close() {
        /* Close the CDB file. */
        try {
            file_.close();
            file_ = null;
        } catch (IOException ignored) {
        }
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
    public final void findinit() {
        loop_ = 0;
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
     * Finds the next record stored under the given key.
     *
     * @param key The key to search for.
     * @return The next record store under the given key, or
     * <code>null</code> if no record with that key could be found.
     */
    public final synchronized byte[] findnext(byte[] key) {
        /* There are no keys if we could not read the slot table. */
		if (slotTable_ == null) {
			return null;
		}

        /* Locate the hash entry if we have not yet done so. */
        if (loop_ == 0) {
            /* Get the hash value for the key. */
            int u = hash(key);

            /* Unpack the information for this record. */
            int slot = u & 255;
            hslots_ = slotTable_[(slot << 1) + 1];
			if (hslots_ == 0) {
				return null;
			}
            hpos_ = slotTable_[slot << 1];

            /* Store the hash value. */
            khash_ = u;

            /* Locate the slot containing this key. */
            u >>>= 8;
            u %= hslots_;
            u <<= 3;
            kpos_ = hpos_ + u;
        }

        /* Search all of the hash slots for this key. */
        try {
            while (loop_ < hslots_) {
                /* Read the entry for this key from the hash slot. */
                file_.seek(kpos_);

                int h = readInt(file_);
                int pos = readInt(file_);
				if (pos == 0) {
					return null;
				}

                /* Advance the loop count and key position.  Wrap the
                 * key position around to the beginning of the hash slot
                 * if we are at the end of the table. */
                loop_ += 1;

                kpos_ += 8;
				if (kpos_ == (hpos_ + (hslots_ << 3))) {
					kpos_ = hpos_;
				}

                /* Ignore this entry if the hash values do not match. */
				if (h != khash_) {
					continue;
				}

                /* Get the length of the key and data in this hash slot
                 * entry. */
                file_.seek(pos);

                int klen = readInt(file_);
				if (klen != key.length) {
					continue;
				}

                int dlen = readInt(file_);

                /* Read the key stored in this entry and compare it to
                 * the key we were given. */
                boolean match = true;
                byte[] k = new byte[klen];
                file_.readFully(k);
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
                file_.readFully(d);
                return d;
            }
        } catch (IOException ignored) {
            return null;
        }

        /* No more data values for this key. */
        return null;
    }

    /**
     * @inheritDoc
     */
    public Iterator<CdbElement> iterator() {
        final int endOfData;
        try {
            file_.seek(0);
            int tmpEod = readInt(file_);
            /* Skip the rest of the hashtable. */
            file_.seek(2048);
            endOfData = tmpEod;
        } catch  (IOException ioe) {
            throw new CdbException("Iterator creation failure", ioe);
        }

        return new Iterator<>() {
            private long lastOffset;
            {
                try {
                    lastOffset = file_.getFilePointer();
                } catch (IOException ioe) {
                    throw new CdbException("Iterator initialization failure", ioe);
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
                    if (lastOffset != file_.getFilePointer()) {
                        file_.seek(lastOffset);
                    }

                    // read key/value lengths
                    int keyLength = readInt(file_);
                    int valueLength = readInt(file_);

                    // read the key
                    byte[] key = new byte[keyLength];
                    if (file_.read(key) != keyLength) {
                        throw new IOException("unrecognizable cdb format");
                    }
                    // read the value
                    byte[] value = new byte[valueLength];
                    if (file_.read(value) != valueLength) {
                        throw new IOException("unrecognizable cdb format");
                    }

                    lastOffset = file_.getFilePointer();
                    return new CdbElement(key, value);
                } catch (IOException ioe) {
                    throw new CdbException("Iterator next failure", ioe);
                }
            }
        };
    }

    private int readInt(DataInput in) throws IOException {
        return (in.readUnsignedByte() & INT_BYTE_MASK)
                | ((in.readUnsignedByte() & INT_BYTE_MASK) << 8)
                | ((in.readUnsignedByte() & INT_BYTE_MASK) << 16)
                | ((in.readUnsignedByte() & INT_BYTE_MASK) << 24);
    }
}
