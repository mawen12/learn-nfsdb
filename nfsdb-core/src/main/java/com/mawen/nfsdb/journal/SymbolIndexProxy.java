package com.mawen.nfsdb.journal;

import java.io.Closeable;
import java.io.IOException;

import com.mawen.nfsdb.journal.column.SymbolIndex;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.logging.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class SymbolIndexProxy<T> implements Closeable {

	private static final Logger LOGGER = Logger.getLogger(SymbolIndexProxy.class);

	private final Partition<T> partition;
	private final int columnIndex;
	private SymbolIndex index;
	private volatile long lastAccessed;
	private long txAddress;

	@Override
	public void close() {
		if (index != null) {
			LOGGER.trace("Closing " + this);
			index.close();
			index = null;
		}
		lastAccessed = 0L;
	}

	public long getLastAccessed() {
		return lastAccessed;
	}

	public void setTxAddress(long txAddress) {
		this.txAddress = txAddress;
		if (index != null) {
			index.setTxAddress(txAddress);
		}
	}

	@Override
	public String toString() {
		return "SymbolIndexProxy{" +
				"index=" + index +
				", lastAccessed=" + lastAccessed +
				'}';
	}

	public int getColumnIndex() {
		return columnIndex;
	}

	SymbolIndex getIndex() throws JournalException {
		lastAccessed = partition.getJournal().getTimerCache().getMillis();
		if (index == null) {
			JournalMetadata<T> meta = partition.getJournal().getMetadata();
			index = new SymbolIndex(
					meta.getColumnIndexBase(partition.getPartitionDir(), columnIndex),
					meta.getColumnMetadata(columnIndex).distinctCountHint,
					meta.getRecordHint(),
					partition.getJournal().getMode(),
					txAddress
			);
		}
		return index;
	}

	SymbolIndexProxy(Partition<T> partition, int columnIndex, long txAddress) {
		this.partition = partition;
		this.columnIndex = columnIndex;
		this.txAddress = txAddress;
	}
}
