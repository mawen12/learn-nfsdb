package com.mawen.nfsdb.journal.utils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public final class Rows {

	public static long toRowID(int partitionIndex, long localRowID) {
		return ((long)partitionIndex << 44L) + localRowID;
	}

	public static int toPartitionIndex(long rowID) {
		return (int)(rowID >>> 44L);
	}

	public static long toLocalRowID(long rowID) {
		return rowID & 0xFFFFFFFFFFFL;
	}

	private Rows() {}
}
