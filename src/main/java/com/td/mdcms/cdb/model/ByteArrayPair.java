/*
 * Copyright (C) 2019 by Teradata Corporation. All Rights Reserved. TERADATA CORPORATION
 * CONFIDENTIAL AND TRADE SECRET
 */
package com.td.mdcms.cdb.model;

import java.util.Arrays;

/**
 * Specialization of a {@link Pair}, for two byte arrays.  This is used in a lot
 * of places so this simplifies things.  It also allows us to override
 * {@code equals} and {@code hashCode}, which is way cool.
 */
public class ByteArrayPair extends Pair<byte[], byte[]> {
    public ByteArrayPair(byte[] first, byte[] second) {
        super(Arrays.copyOf(first, first.length),
                Arrays.copyOf(second, second.length));
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof ByteArrayPair) &&
                Arrays.equals(this.first, ((ByteArrayPair)o).first) &&
                Arrays.equals(this.second, ((ByteArrayPair)o).second);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + Arrays.hashCode(first);
        hash = 31 * hash + Arrays.hashCode(second);
        return hash;
    }
}
