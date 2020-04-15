package com.strangegizmo.cdb.internal;

import com.strangegizmo.cdb.model.Pair;

/**
 * Specific {@link Pair} for two integers, since that is used a <em>lot</em>.
 */
public class IntPair extends Pair<Integer, Integer> {
    public IntPair(Integer first, Integer second) {
        super(first, second);
    }
}
