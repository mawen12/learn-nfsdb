package com.mawen.nfsdb.journal.collections;

import gnu.trove.list.array.TLongArrayList;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class LongArrayList extends TLongArrayList {

	public void setCapacity(int capacity) {
		if (capacity > _data.length) {
			long[] tmp = new long[capacity];
			System.arraycopy(_data, 0, tmp, 0, _data.length);
			_data = tmp;
		}
	}

	public void add(LongArrayList that) {
		int sz = that.size();
		setCapacity(sz);
		add(that._data, 0, sz);
	}

	public void setPos(int pos) {
		_pos = pos;
	}
}
