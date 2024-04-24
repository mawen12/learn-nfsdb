package com.mawen.nfsdb.journal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.mawen.nfsdb.journal.column.AbstractColumn;
import com.mawen.nfsdb.journal.column.FixedWidthColumn;
import com.mawen.nfsdb.journal.column.MappedFileImpl;
import com.mawen.nfsdb.journal.column.NullsColumn;
import com.mawen.nfsdb.journal.column.SymbolIndex;
import com.mawen.nfsdb.journal.column.VarcharColumn;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.factory.NullsAdaptor;
import com.mawen.nfsdb.journal.logging.Logger;
import com.mawen.nfsdb.journal.utils.Dates;
import com.mawen.nfsdb.journal.utils.Rows;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class Partition<T> implements Iterable<T>, Closeable {
	private static final Logger LOGGER = Logger.getLogger(Partition.class);
	private final Journal<T> journal;
	private final ArrayList<SymbolIndexProxy<T>> indexProxies = new ArrayList<>();
	private final ArrayList<SymbolIndexProxy<T>> columnIndexProxies = new ArrayList<>();
	private final Interval interval;
	private final BitSet nulls;
	private final NullsAdaptor<T> nullsAdaptor;
	private AbstractColumn[] columns;
	private NullsColumn nullsColumn;
	private int partitionIndex;
	private File partitionDir;
	private long lastAccessed = System.currentTimeMillis();
	private long txLimit;

	public NullsColumn getNullsColumn() {
		return nullsColumn;
	}

	public Partition<T> open() throws JournalException {

	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public Iterator<T> iterator() {
		return null;
	}

	public ParallelIterator<T> parallelIterator() {

	}

	public void compact() throws JournalException {
		if (columns == null || columns.length == 0) {
			throw new JournalException("Cannot compact closed partition: %s", this);
		}

		for (int i = 0, columnsLength = columns.length; i < columnsLength; i++) {
			AbstractColumn col = columns[i];
			if (col != null) {
				col.compact();
			}
		}

		for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
			SymbolIndexProxy<T> proxy = indexProxies.get(i);
			proxy.getIndex().compact();
		}
	}

	public void updateIndexes(long oldSize, long newSize) {
		if (oldSize < newSize) {
			try {
				for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
					SymbolIndexProxy<T> proxy = indexProxies.get(i);
					SymbolIndex index = proxy.getIndex();
					FixedWidthColumn col = getFixedWidthColumn(proxy.getColumnIndex());
					for (long k = oldSize; k < newSize; k++) {
						index.put(col.getInt(k), k);
					}
					index.commit();
				}
			}
			catch (JournalException e) {
				throw new JournalRuntimeException(e);
			}
		}
	}

	void clearTx() {
		applyTx(Journal.TX_LIMIT_EVAL, null);
	}

	void setPartitionDir(File partitionDir, long[] indexTxAddresses) {
		boolean create = partitionDir != null && !partitionDir.equals(this.partitionDir);
		this.partitionDir = partitionDir;
		if (create) {
			createSymbolIndexProxies(indexTxAddresses);
		}
	}

	Partition<T> access() {
		this.lastAccessed = getJournal().getTimerCache().getMillis();
		return this;
	}

	void truncate(long newSize) throws JournalException {
		if (isOpen() && size() > newSize) {
			for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
				SymbolIndexProxy<T> proxy = indexProxies.get(i);
				proxy.getIndex().truncate(newSize);
			}
			for (AbstractColumn column : columns) {
				if (column != null) {
					column.truncate(newSize);
				}
			}

			commitColumn();
			clearTx();
		}
	}

	void expireOpenIndices() throws IOException {
		 long expiry = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(journal.getMetadata().getOpenPartitionTTL());
		for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
			SymbolIndexProxy<T> proxy = indexProxies.get(i);
			if (expiry > proxy.getLastAccessed()) {
				proxy.close();
			}
		}
	}

	void getIndexPointers(long[] pointers) throws JournalException {
		for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
			SymbolIndexProxy<T> proxy = indexProxies.get(i);
			pointers[proxy.getColumnIndex()] = proxy.getIndex().getTxAddress();
		}
	}

	void commit() throws JournalException{
		for (int i = 0, indexProxiesSize = indexProxies.size(); i < indexProxiesSize; i++) {
			SymbolIndexProxy<T> proxy = indexProxies.get(i);
			proxy.getIndex().commit();
		}
	}

	private FixedWidthColumn getFixedWidthColumn(int i) {
		checkColumnIndex(i);
		return (FixedWidthColumn) columns[i];
	}

	private FixedWidthColumn getFixedColumnOrNPE(long localRowID, int columnIndex) {
		FixedWidthColumn result = getFixedColumnOrNull(localRowID, columnIndex);
		if (result == null) {
			throw new NullPointerException("NULL value for " + journal.getMetadata().getColumnMetadata(columnIndex).name + "; rowID [" + Rows.toRowID(partitionIndex, localRowID) + "]");
		}
		return result;
	}

	private FixedWidthColumn getFixedColumnOrNull(long localRowID, int columnIndex) {
		if (getNullsColumn().getBitSet(localRowID).get(columnIndex)) {
			return null;
		}
		else {
			return getFixedWidthColumn(columnIndex);
		}
	}

	private void open(int columnIndex) {

		JournalMetadata.ColumnMetadata m = journal.getMetadata().getColumnMetadata(columnIndex);
		switch (m.type) {
			case STRING:
				columns[columnIndex] = new VarcharColumn(
					new MappedFileImpl(new File(partitionDir, m.name + ".d"), m.bitHint, journal.getMode()),
					new MappedFileImpl(new File(partitionDir, m.name + ".i"), m.indexBitHint, journal.getMode()),
					m.maxSize
				);
				break;
			default:
				columns[columnIndex] = new FixedWidthColumn(
						new MappedFileImpl(new File(partitionDir, m.name + ".d"), m.bitHint, journal.getMode()),
						m.size
				);
		}
	}

	private void checkColumnIndex(int i) {
		if (columns == null) {
			throw new JournalRuntimeException("Partition is closed: %s", this);
		}

		if (i < 0 || i >= columns.length) {
			throw new JournalRuntimeException("Invalid column index: %d in %s", i, this);
		}
	}

	private void createSymbolIndexProxies(long[] indexTxAddresses) {
		indexProxies.clear();
		columnIndexProxies.clear();
		JournalMetadata<T> meta = journal.getMetadata();
		for (int i = 0; i < meta.getColumnCount(); i++) {
			if (meta.getColumnMetadata(i).indexed) {
				SymbolIndexProxy<T> proxy = new SymbolIndexProxy<>(this, i, indexTxAddresses == null ? 0 : indexTxAddresses[i]);
				indexProxies.add(proxy);
				columnIndexProxies.add(proxy);
			}
			else {
				columnIndexProxies.add(null);
			}
		}
	}

	@Override
	public String toString() {
		return "Partition{" +
				"partitionIndex=" + partitionIndex +
				", open=" + isOpen() +
				", partitionDir=" + partitionDir +
				", interval=" + interval +
				", lastAccessed=" + lastAccessed +
				'}';
	}

	Partition(Journal<T> journal, Interval interval, int partitionIndex, long txLimit, long[] indexTxAddresses) {
		this.journal = journal;
		this.partitionIndex = partitionIndex;
		this.interval = interval;
		this.txLimit = txLimit;
		this.nulls = new BitSet(journal.getMetadata().getColumnCount());
		this.nullsAdaptor = journal.getMetadata().getNullsAdaptor();

		String dateStr = Dates.dirNameForIntervalStart(interval, journal.getMetadata.getPartitionType());
		if (dateStr.length() > 0) {
			setPartitionDir(new File(this.journal.getLocation(), dateStr), indexTxAddresses);
		}
		else {
			setPartitionDir(this.journal.getLocation(), indexTxAddresses);
		}
	}
}
