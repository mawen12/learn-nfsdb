package com.mawen.nfsdb.journal.tx;

import java.util.Arrays;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class Tx {

	public static final byte TX_NORMAL = 0;
	// 8
	public long prevTxAddress;
	// 1
	public byte command;
	// 8
	public long timestamp;
	// 8
	public long journalMaxRowID;
	// 8
	public long lastPartitionTimestamp;
	// 8
	public long lagSize;
	// 1 + 1 + 64
	public String lagName;
	// 2 + 4 * symbolTableSizes.len
	public int[] symbolTableSizes;
	// 2 + 8 * symbolTableIndexPointers.len
	public long[] symbolTableIndexPointers;
	// 2 + 8 * indexPointers.len
	public long[] indexPointers;
	// 2 + 8 * lagIndexPointers.len
	public long[] lagIndexPointers;

	@Override
	public String toString() {
		return "Tx{" +
				"prevTxAddress=" + prevTxAddress +
				", command=" + command +
				", timestamp=" + timestamp +
				", journalMaxRowID=" + journalMaxRowID +
				", lastPartitionTimestamp=" + lastPartitionTimestamp +
				", lagSize=" + lagSize +
				", lagName='" + lagName + '\'' +
				", symbolTableSizes=" + Arrays.toString(symbolTableSizes) +
				", symbolTableIndexPositions=" + Arrays.toString(symbolTableIndexPointers) +
				", indexPointers=" + Arrays.toString(indexPointers) +
				", lagIndexPointers=" + Arrays.toString(lagIndexPointers) +
				", size " + size() +
				'}';
	}

	public int size() {
		return 8 + 1 + 8 + 8 + 8 + 8
				+ 1 + 1 + 64
				+ 2 + 4 * (symbolTableSizes == null ? 0 : symbolTableSizes.length)
				+ 2 + 8 * (symbolTableIndexPointers == null ? 0 : symbolTableIndexPointers.length)
				+ 2 + 8 * (indexPointers == null ? 0 : indexPointers.length)
				+ 2 + 8 * (lagIndexPointers == null ? 0 : lagIndexPointers.length);
	}
}
