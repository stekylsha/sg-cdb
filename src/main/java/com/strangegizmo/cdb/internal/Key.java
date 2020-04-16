package com.strangegizmo.cdb.internal;

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
}
