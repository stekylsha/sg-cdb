package com.strangegizmo.cdb.internal;

import java.nio.IntBuffer;

import com.strangegizmo.cdb.exception.CdbStateException;

public class SlotTableInfo {
    private final Key key;
    private final IntPair offsetEntries;

    public SlotTableInfo(IntBuffer mainTable, Key key) {
        this.key = key;

        // Get slot table info
        int tableSlot = (key.hash & 0x00ff) << 1; // (h % 256) * 2 entries per slot
        int[] slotTableValues = new int[2]; // subtable index +  number of entries
        mainTable.duplicate()
                .position(tableSlot)
                .get(slotTableValues);
        this.offsetEntries = new IntPair(slotTableValues[0], slotTableValues[1]);
    }

    public Key getKey() {
        return key;
    }

    public int offset() {
        return offsetEntries.first;
    }

    public int entries() {
        return offsetEntries.second;
    }

    public int firstEntry() {
        if (!hasEntries()) {
            throw new CdbStateException("No entries exist for key '" +
                    new String(key.key) + "'");
        }
        return (key.hash >>> 8) % offsetEntries.second;
    }

    public boolean hasEntries() {
        return offsetEntries.second != 0;
    }
}
