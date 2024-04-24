package com.mawen.nfsdb.journal.factory;

import java.util.BitSet;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public interface NullsAdaptor<T> {

	void setNulls(T obj, BitSet src);

	void getNulls(T obj, BitSet dst);

	void clear(T obj);
}
