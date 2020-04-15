package com.strangegizmo.cdb.model;

/**
 * Since there isn't a {@code Pair} type in {@code java.util} and we
 * don't want to add dependencies, a simple structure to maintain an immutable
 * tuple.
 *
 * @param <S> The type of the first entry.
 * @param <T> The type of the second entry.
 */
public class Pair<S, T> {
    public final S first;
    public final T second;

    public Pair(S first, T second) {
        this.first = first;
        this.second = second;
    }
}
