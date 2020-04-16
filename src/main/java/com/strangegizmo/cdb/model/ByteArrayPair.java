package com.strangegizmo.cdb.model;

import java.util.Arrays;

/**
 * Since there isn't a {@code Pair} type in {@code java.util} and we
 * don't want to add dependencies, a simple structure to maintain an immutable
 * tuple.
 *
 * @param <S> The type of the first entry.
 * @param <T> The type of the second entry.
 */
public class ByteArrayPair extends Pair<byte[], byte[]> {
    public ByteArrayPair(byte[] first, byte[] second) {
        super(Arrays.copyOf(first, first.length),
                Arrays.copyOf(second, second.length));
    }
}
