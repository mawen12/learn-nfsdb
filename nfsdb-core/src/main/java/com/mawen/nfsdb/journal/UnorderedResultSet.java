package com.mawen.nfsdb.journal;

import com.mawen.nfsdb.journal.collections.LongArrayList;

/**
 * Unordered result set does not guarantee ROWIDs to be in ascending order.
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class UnorderedResultSet<T> extends ResultSet<T> {

	public UnorderedResultSet(Journal<T> journal, LongArrayList rowIDs) {
		super(journal, rowIDs);
	}
}
