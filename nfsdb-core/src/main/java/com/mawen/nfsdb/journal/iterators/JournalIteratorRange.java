package com.mawen.nfsdb.journal.iterators;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public final class JournalIteratorRange {
	final int partitionID;
	final long upperRowIDBound;
	final long lowerRowIDBound;

	public JournalIteratorRange(int partitionID, long upperRowIDBound, long lowerRowIDBound) {
		this.partitionID = partitionID;
		this.upperRowIDBound = upperRowIDBound;
		this.lowerRowIDBound = lowerRowIDBound;
	}
}
