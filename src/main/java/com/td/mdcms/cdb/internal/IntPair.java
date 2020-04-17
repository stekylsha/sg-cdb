/*
 * Copyright (C) 2019 by Teradata Corporation. All Rights Reserved. TERADATA CORPORATION
 * CONFIDENTIAL AND TRADE SECRET
 */
package com.td.mdcms.cdb.internal;

import com.td.mdcms.cdb.model.Pair;

/**
 * Specific {@link Pair} for two integers, since that is used a <em>lot</em>.
 */
public class IntPair extends Pair<Integer, Integer> {
    public IntPair(Integer first, Integer second) {
        super(first, second);
    }
}
