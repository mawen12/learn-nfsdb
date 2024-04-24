package com.mawen.nfsdb.journal;

import java.util.Iterator;

import com.mawen.nfsdb.journal.utils.Rows;
import gnu.trove.list.TLongList;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class ResultSet<T> implements Iterable<T> {

	private final Journal<T> journal;
	private final TLongList rowIDs;

	public

	@Override
	public Iterator<T> iterator() {
		return null;
	}

	/////////////////////////////////////////////////////////////////

	void quickSort(Order order, int lo, int hi, int... columnIndices) {

		if (lo >= hi) {
			return;
		}

		int pIndex = lo + (hi - lo) / 2;
		long pivot = rowIDs.get(pIndex);

		int multiplier = 1;

		if (order == Order.DESC) {
			multiplier = -1;
		}

		int i = lo;
		int j = hi;

		while (i <= j) {

			while (multiplier * compare(journal, columnIndices, rowIDs.get(i), pivot) < 0) {
				i++;
			}

			while (multiplier * compare(journal, columnIndices, pivot, rowIDs.get(j)) < 0) {
				j--;
			}

			if (i <= j) {
				long temp = rowIDs.get(i);
				rowIDs.set(i, rowIDs.get(j));
				rowIDs.set(j, temp);
				i++;
				j--;
			}
		}

		quickSort(order, lo, j, columnIndices);
		quickSort(order, i, hi, columnIndices);
	}

	/////////////////////////////////////////////////////////////////

	private static <T> int compare(Journal<T> journal, int[] columns, long rightRowID, long leftRowID) {
		int result = 0;
		long leftLocalRowID = Rows.toLocalRowID(leftRowID);
		long rightLocalRowID = Rows.toLocalRowID(rightRowID);

		Partition<T> leftPart = journal.getPartition(Rows.toPartitionIndex(leftRowID), true);
		Partition<T> rightPart = journal.getPartition(Rows.toPartitionIndex(rightRowID), true);

		for (int column : columns) {
			journal.getColumnMetadata(column);
		}
	}

	private static int compare(int a, int b) {
		if (a == b) {
			return 0;
		}
		else if (a > b) {
			return 1;
		}
		else {
			return -1;
		}
	}

	private static int compare(double a, double b) {
		if (a == b) {
			return 0;
		}
		else if (a > b) {
			return 1;
		}
		else {
			return -1;
		}
	}

	private static int compare(long a, long b) {
		if (a == b) {
			return 0;
		}
		else if (a > b) {
			return 1;
		}
		else {
			return -1;
		}
	}

	private int[] getColumnIndexes(String... columnNames) {
		int[] columnIndices = new int[columnNames.length];
		for (int i = 0, columnNamesLength = columnNames.length; i < columnNamesLength; i++) {
			columnIndices[i] = journal.getMetadata().getColumnIndex(columnNames[i]);
		}
		return columnIndices;
	}

	/////////////////////////////////////////////////////////////////

	ResultSet(Journal<T> journal, TLongList rowIDs) {
		this.journal = journal;
		this.rowIDs = rowIDs;
	}

	/////////////////////////////////////////////////////////////////

	public enum Order {
		ASC, DESC
	}
}
