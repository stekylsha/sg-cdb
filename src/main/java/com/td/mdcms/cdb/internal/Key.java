/*
 * Copyright (C) 2019 by Teradata Corporation. All Rights Reserved. TERADATA CORPORATION
 * CONFIDENTIAL AND TRADE SECRET
 */
package com.td.mdcms.cdb.internal;

public class Key {
    /**
     * Initial hash value
     */
    private static final int INITIAL_HASH = 5381;

    public final byte[] key;
    public final int hash;

    public Key(byte[] key) {
        this.key = key;
        this.hash = hash(key);
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

    /**
     * Convenience for getting the {@code hash % 256}.
     * @return The last two bytes of the hash.
     */
    public int hashMod256() {
        return (hash & 0x00ff);
    }

    /**
     * Convenience for getting the {@code hash / 256}.
     * @return The hash divided by 256.
     */
    public int hashDiv256() {
        return (hash >>> 8);
    }
}
