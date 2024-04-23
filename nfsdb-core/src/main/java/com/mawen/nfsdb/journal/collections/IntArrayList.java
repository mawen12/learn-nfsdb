package com.mawen.nfsdb.journal.collections;

import gnu.trove.list.array.TIntArrayList;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class IntArrayList extends TIntArrayList {

	public IntArrayList() {
	}

	public IntArrayList(IntArrayList that) {
		super();
		add(that);
	}

	public IntArrayList(int capacity) {
		super(capacity);
	}


	public void setCapacity(int capacity) {
		if (capacity > _data.length) {
			int[] tmp = new int[capacity];
			System.arraycopy(_data, 0, tmp, 0, _data.length);
			_data = tmp;
		}
	}

	public void add(IntArrayList that) {
		int sz = that.size();
		setCapacity(sz);
		add(that._data, 0, sz);
	}
}
