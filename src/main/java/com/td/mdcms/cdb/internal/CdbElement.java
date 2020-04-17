/*
 * Copyright (C) 2019 by Teradata Corporation. All Rights Reserved. TERADATA CORPORATION
 * CONFIDENTIAL AND TRADE SECRET
 */
package com.td.mdcms.cdb.internal;

/**
 * CdbElement represents a single element in a constant database.
 *
 * @author		Michael Alyn Miller <malyn@strangeGizmo.com>
 * @version		1.0.2
 */
public final class CdbElement {
	/** The key value for this element. */
	private final byte[] key;

	/** The data value for this element. */
	private final byte[] data;


	/**
	 * Creates an instance of the CdbElement class and initializes it
	 * with the given key and data values.
	 *
	 * @param key The key value for this element.
	 * @param data The data value for this element.
	 */
	public CdbElement(byte[] key, byte[] data) {
		this.key = key;
		this.data = data;
	}


	/**
	 * Returns this element's key.
	 *
	 * @return This element's key.
	 */
	public final byte[] getKey() {
		return key;
	}

	/**
	 * Returns this element's data.
	 *
	 * @return This element's data.
	 */
	public final byte[] getData() {
		return data;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "CdbElement{" +
				"key=" + new String(key) +
				", data=" + new String(data) +
				'}';
	}
}
