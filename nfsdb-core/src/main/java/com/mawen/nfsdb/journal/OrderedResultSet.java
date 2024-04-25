package com.mawen.nfsdb.journal;

import com.mawen.nfsdb.journal.collections.LongArrayList;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.utils.Rows;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class OrderedResultSet<T> extends ResultSet<T> {

	public OrderedResultSet(Journal<T> journal, LongArrayList rowIDs) {
		super(journal, rowIDs);
	}

	public long getMaxTimestamp() throws JournalException {
		if (size() == 0) {
			return 0;
		}

		long rowID = getRowID(size() - 1);
		int timestampColumnIndex = getJournal().getMetadata().getTimestampColumnIndex();
		return getJournal().getPartition(Rows.toPartitionIndex(rowID), true).getLong(Rows.toLocalRowID(rowID), timestampColumnIndex);
	}
}
