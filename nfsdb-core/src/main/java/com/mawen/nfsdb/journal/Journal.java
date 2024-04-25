package com.mawen.nfsdb.journal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.mawen.nfsdb.journal.column.ColumnType;
import com.mawen.nfsdb.journal.column.FixedWidthColumn;
import com.mawen.nfsdb.journal.column.SymbolTable;
import com.mawen.nfsdb.journal.concurrent.TimerCache;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.exceptions.JournalUnSupportedTypeException;
import com.mawen.nfsdb.journal.factory.JournalClosingListener;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.iterators.JournalIterator;
import com.mawen.nfsdb.journal.iterators.ParallelIterator;
import com.mawen.nfsdb.journal.locks.Lock;
import com.mawen.nfsdb.journal.locks.LockManager;
import com.mawen.nfsdb.journal.logging.Logger;
import com.mawen.nfsdb.journal.query.api.Query;
import com.mawen.nfsdb.journal.query.spi.QueryImpl;
import com.mawen.nfsdb.journal.tx.Tx;
import com.mawen.nfsdb.journal.tx.TxLog;
import com.mawen.nfsdb.journal.utils.Dates;
import com.mawen.nfsdb.journal.utils.Rows;
import com.mawen.nfsdb.journal.utils.Unsafe;
import gnu.trove.list.TLongList;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class Journal<T> implements Iterable<T>, Closeable {

	private static final Logger LOGGER = Logger.getLogger(Journal.class);
	public static final long TX_LIMIT_EVAL = -1L;
	protected final List<Partition<T>> partitions = new ArrayList<>();
	protected TxLog txLog;
	private final JournalMetadata<T> metadata;
	private final File location;
	private final Map<String, SymbolTable> symbolTableMap = new HashMap<>();
	private final ArrayList<SymbolTable> symbolTables = new ArrayList<>();
	private final JournalKey<T> key;
	private final Query<T> query = new QueryImpl<>(this);
	private final TimerCache timerCache;
	private final long timestampOffset;
	private final Comparator<T> timestampComparator = new Comparator<>() {
		@Override
		public int compare(T o1, T o2) {
			long x = Unsafe.getUnsafe().getLong(o1, timestampOffset);
			long y = Unsafe.getUnsafe().getLong(o2, timestampOffset);
			return (x < y) ? -1 : (x == y) ? 0 : 1;
		}
	};
	private boolean open;
	private String[] readColumns;
	private ColumnMetadata[] columnMetadata;
	private Partition<T> irregularPartition;
	private JournalClosingListener closeListener;

	public Journal(JournalKey<T> key, JournalMetadata<T> metadata, TimerCache timerCache) throws JournalException {
		this.metadata = metadata;
		this.key = key;
		this.location = new File(metadata.getLocation());
		this.timerCache = timerCache;
		this.txLog = new TxLog(location, getMode());
		this.open = true;
		this.timestampOffset = getMetadata().getTimestampColumnMetadata() == null ? -1 : getMetadata().getTimestampColumnMetadata().offset;
		configure();
	}

	public void setCloseListener(JournalClosingListener closeListener) {
		this.closeListener = closeListener;
	}

	public long getTimestamp(T o) {
		return Unsafe.getUnsafe().getLong(o, timestampOffset);
	}

	@Override
	public void close() throws IOException {
		if (open) {

			if (closeListener != null) {
				if (!closeListener.closing(this)) {
					return;
				}
			}

			closePartitions();
			for (SymbolTable tab : symbolTables) {
				tab.close();
			}
			txLog.close();
			open = false;
		}
		else {
			throw new JournalRuntimeException("Already closed: %s", this);
		}
	}

	public boolean refresh() throws JournalException {
		if (txLog.hasNext()) {
			Tx tx = txLog.get();
			refresh(tx);
			for (int i = 0; i < symbolTables.size(); i++) {
				symbolTables.get(i).applyTx(tx.symbolTableSizes[i], tx.symbolTableIndexPointers[i]);
			}
			return true;
		}
		return false;
	}

	public int getSymbolTableCount() {
		return symbolTables.size();
	}

	public SymbolTable getSymbolTable(String columnName) {
		SymbolTable result = symbolTableMap.get(columnName);
		if (result == null) {
			throw new JournalRuntimeException("Column is not a symbol: %s", columnName);
		}
		return result;
	}

	public SymbolTable getSymbolTable(int index) {
		return symbolTables.get(index);
	}

	public <X> X iteratePartitions(AbstractResultSetBuilder<T, X> builder) throws JournalException {
		builder.setJournal(this);
		int count = getPartitionCount();
		for (int i = 0; i < count; i++) {
			if (builder.next(getPartition(i, false), true)) {
				break;
			}
		}
		return builder.getResult();
	}

	public int getPartitionCount() {
		if (irregularPartition == null) {
			return nonLagPartitionCount();
		}
		else {
			return nonLagPartitionCount() + 1;
		}
	}

	public Partition<T> getPartition(int partitionIndex, boolean open) throws JournalException {
		if (irregularPartition != null && partitionIndex == nonLagPartitionCount()) {
			return open ? irregularPartition.open() : irregularPartition;
		}

		Partition<T> partition = partitions.get(partitionIndex).access();
		if (open) {
			partition.open();
		}
		return partition;
	}

	public int nonLagPartitionCount() {
		return partitions.size();
	}

	/**
	 * Set the name of all the columns that have to be read (only in RO flow mode).
	 *
	 * @param readColumns the names of all the columns that have to be read.
	 */
	public Journal<T> setReadColumns(String... readColumns) {
		this.readColumns = readColumns;
		return this;
	}

	public <X> X iteratePartitionsDesc(AbstractResultSetBuilder<T, X> builder) throws JournalException {
		builder.setJournal(this);
		int count = getPartitionCount();
		for (int i = count; i >= 0; i--) {
			if (builder.next(getPartition(i, false), false)) {
				break;
			}
		}
		return builder.getResult();
	}

	public Partition<T> lagNonEmptyNonLag() throws JournalException {

		if (nonLagPartitionCount() > 0) {

			Partition<T> result = getPartition(nonLagPartitionCount() - 1, true);

			while (true) {
				if (result.size() > 0) {
					return result;
				}

				if (result.getPartitionIndex() == 0) {
					break;
				}

				result = getPartition(result.getPartitionIndex() - 1, true);
			}

			return result;
		}
		else {
			return null;
		}
	}

	public Partition<T> getIrregularPartition() {
		return irregularPartition;
	}

	public void setIrregularPartition(Partition<T> partition) {
		removeIrregularPartitionInternal();
		irregularPartition = partition;
		irregularPartition.setPartitionIndex(nonLagPartitionCount());
	}

	public boolean hasIrregularPartition() {
		return irregularPartition != null;
	}

	public void expireOpenFiles() {
		long expiry = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(getMetadata().getOpenPartitionTTL());
		for (int i = 0, partitionSize = partitions.size(); i < partitionSize; i++) {
			Partition<T> partition = partitions.get(i);
			if (expiry > partition.getLastAccessed() && partition.isOpen()) {
				partition.close();
			}
			else {
				partition.expireOpenIndices();
			}
		}
	}

	public void read(long rowID, T obj) throws JournalException {
		getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID), obj);
	}

	public T[] read(TLongList rowIDs) throws JournalException {
		T[] result = (T[]) Array.newInstance(metadata.getModelClass(), rowIDs.size());
		for (int i = 0; i < rowIDs.size(); i++) {
			result[i] = read(rowIDs.get(i));
		}
		return result;
	}

	public T read(long rowID) throws JournalException {
		return getPartition(Rows.toPartitionIndex(rowID), true).read(Rows.toLocalRowID(rowID));
	}

	public boolean isOpen() {
		return open;
	}

	public long size() throws JournalException {
		long result = 0;
		for (int i = 0; i < getPartitionCount(); i++) {
			result += getPartition(i, true).size();
		}
		return result;
	}

	public JournalKey<T> getKey() {
		return key;
	}

	@Override
	public String toString() {
		return getClass().getName() + "[" +
				"location=" + location +
				", mode=" + getMode() +
				", metadata=" + metadata +
				']';
	}

	/**
	 * Get the Journal's flow model (the probable query style)
	 *
	 * @return the Journal's flow model
	 */
	public JournalMode getMode() {
		return JournalMode.READ;
	}

	public long incrementRowID(long rowID) throws JournalException {
		int partitionIndex = Rows.toPartitionIndex(rowID);
		long localRowID = Rows.toLocalRowID(rowID);

		Partition p = getPartition(partitionIndex, open);
		if (localRowID < p.size() - 1) {
			return Rows.toRowID(partitionIndex, localRowID + 1);
		}

		while (++partitionIndex < getPartitionCount()) {
			p = getPartition(partitionIndex, true);
			if (p.size() > 0) {
				return Rows.toRowID(partitionIndex, 0);
			}
		}

		return -1;
	}

	public long decrementRowID(long rowID) throws JournalException {
		int partitionIndex = Rows.toPartitionIndex(rowID);
		long localRowID = Rows.toLocalRowID(rowID);

		if (localRowID > 0) {
			return Rows.toRowID(partitionIndex, localRowID - 1);
		}

		while (--partitionIndex >= 0) {
			Partition p = getPartition(partitionIndex, true);
			if (p.size() > 0) {
				return Rows.toRowID(partitionIndex, p.size() - 1);
			}
		}

		return -1;
	}

	public TempPartition<T> createTempPartition(String name) {
		int lag = getMetadata().getLagHours();
		if (lag <= 0) {
			throw new JournalRuntimeException("Journal doesn't support temp partitions: %s", this);
		}

		Interval interval = null;
		if (getMetadata().getPartitionType() != PartitionType.NONE) {
			if (nonLagPartitionCount() > 0) {
				Interval lastPartitionInterval = partitions.get(nonLagPartitionCount() - 1).getInterval();
				interval = new Interval(lastPartitionInterval.getStart(), lastPartitionInterval.getEnd().plusHours(lag));
			}
			else {
				interval = Dates.intervalForDate(System.currentTimeMillis(), getMetadata().getPartitionType());
			}
		}

		return new TempPartition<>(this, interval, nonLagPartitionCount(), name);
	}

	public JournalMetadata<T> getMetadata() {
		return metadata;
	}

	public Comparator<T> getTimestampComparator() {
		return timestampComparator;
	}

	/**
	 * Get the disk location of the Journal.
	 */
	public File getLocation() {
		return location;
	}

	/**
	 * Is the specified Journal type compatible with this one.
	 */
	public boolean isCompatible(Journal<T> that) {
		return this.getMetadata().getModelClass().equals(that.getMetadata().getModelClass());
	}

	public long getMaxRowID() throws JournalException {
		Partition<T> p = lastNonEmpty();
		if (p == null) {
			return -1;
		}
		else {
			return Rows.toRowID(p.getPartitionIndex(), p.size() - 1);
		}
	}

	public Partition<T> lastNonEmpty() throws JournalException {
		Partition<T> result;

		if (getPartitionCount() == 0) {
			return null;
		}

		if (irregularPartition != null) {
			result = irregularPartition.open();
		}
		else {
			result = partitions.get(partitions.size() - 1).open();
		}

		Partition<T> intermediate = result;
		while (true) {
			if (intermediate.size() > 0) {
				return intermediate;
			}

			if (intermediate.getPartitionIndex() == 0) {
				break;
			}

			intermediate = getPartition(intermediate.getPartitionIndex() - 1, true);
		}

		return result.size() > 0 ? result : null;
	}

	public long getMaxTimestamp() throws JournalException {

		Partition<T> p = lastNonEmpty();
		if (p == null) {
			return 0;
		}

		FixedWidthColumn column = p.getTimestampColumn();
		if (column.size() > 0) {
			return column.getLong(column.size() - 1);
		}
		else {
			return 0;
		}
	}

	public T newObject() {
		return (T) getMetadata().newObject();
	}

	public void clearObject(T obj) {
		for (int i = 0, count = metadata.getColumnCount(); i < count; i++) {
			JournalMetadata.ColumnMetadata m = metadata.getColumnMetadata(i);
			metadata.getNullsAdapter().clear(obj);
			switch (m.type) {
				case BOOLEAN:
					Unsafe.getUnsafe().putBoolean(obj, m.offset, false);
					break;
				case BYTE:
					Unsafe.getUnsafe().putByte(obj, m.offset, (byte) 0);
					break;
				case DOUBLE:
					Unsafe.getUnsafe().putDouble(obj, m.offset, 0d);
					break;
				case INT:
					Unsafe.getUnsafe().putInt(obj, m.offset, 0);
					break;
				case LONG:
					Unsafe.getUnsafe().putLong(obj, m.offset, 0L);
					break;
				case STRING:
				case SYMBOL:
					Unsafe.getUnsafe().putObject(obj, m.offset, null);
					break;
				default:
					throw new JournalUnSupportedTypeException(m.type);
			}
		}
	}

	public ColumnMetadata getColumnMetadata(int columnIndex) {
		return columnMetadata[columnIndex];
	}

	@Override
	public Iterator<T> iterator() {
		return query().all().iterator();
	}

	public Query<T> query() {
		return query;
	}

	public JournalIterator<T> bufferedIterator() {
		return query().all().bufferedIterator();
	}

	public ParallelIterator<T> parallelIterator() {
		return query().all().parallelIterator();
	}

	/////////////////////////////////////////////////////////////////

	protected long getTimestampOffset() {
		return timestampOffset;
	}

	protected void closePartitions() {
		if (irregularPartition != null) {
			irregularPartition.close();
		}
		for (Partition<T> p : partitions) {
			p.close();
		}
		partitions.clear();
	}

	protected void configure() throws JournalException {
		Tx tx = txLog.get();
		configureColumns(tx);
		configureSymbolTableSynonyms();
		configurePartitions(tx);
	}

	protected void removeIrregularPartitionInternal() {
		if (irregularPartition != null) {
			if (irregularPartition.isOpen()) {
				irregularPartition.close();
			}
			irregularPartition = null;
		}
	}

	/////////////////////////////////////////////////////////////////

	String[] getReadColumns() {
		return readColumns;
	}

	TimerCache getTimerCache() {
		return timerCache;
	}

	/**
	 * Replaces current Lag partition, which is cached in this instance of Partition Manager with Lag partition,
	 * which was written to _lag file by another process.
	 */
	void refresh(Tx tx) throws JournalException {

		assert tx != null;

		int txPartitionIndex = Rows.toPartitionIndex(tx.journalMaxRowID);
		if (partitions.size() != txPartitionIndex + 1 || tx.journalMaxRowID == 0) {
			if (tx.journalMaxRowID == 0 || partitions.size() > txPartitionIndex + 1) {
				closePartitions();
			}
			configurePartitions(tx);
		}
		else {
			long txPartitionSize = Rows.toLocalRowID(tx.journalMaxRowID);
			Partition<T> partition = partitions.get(txPartitionIndex);
			partition.applyTx(txPartitionSize, tx.indexPointers);
			configureIrregularPartition(tx);
		}
	}

	/////////////////////////////////////////////////////////////////

	private void configureColumns(Tx tx) throws JournalException {
		int columnCount = getMetadata().getColumnCount();
		columnMetadata = new ColumnMetadata[columnCount];
		for (int i = 0; i < columnCount; i++) {
			columnMetadata[i] = new ColumnMetadata();
			JournalMetadata.ColumnMetadata meta = metadata.getColumnMetadata(i);
			if (meta.type == ColumnType.SYMBOL && meta.sameAs == null) {
				int tabIndex = symbolTables.size();
				int tabSize = tx.symbolTableSizes.length > tabIndex ? tx.symbolTableSizes[tabIndex] : 0;
				long indexTxAddress = tx.symbolTableIndexPointers.length > tabIndex ? tx.symbolTableIndexPointers[tabIndex] : 0;
				SymbolTable tab = new SymbolTable(meta.distinctCountHint, meta.maxSize, location, meta.name, getMode(), tabSize, indexTxAddress);
				symbolTables.add(tab);
				symbolTableMap.put(meta.name, tab);
				columnMetadata[i].symbolTable = tab;
			}
			columnMetadata[i].meta = meta;
		}
	}

	private void configureSymbolTableSynonyms() {
		for (int i = 0, columnCount = getMetadata().getColumnCount(); i < columnCount; i++) {
			JournalMetadata.ColumnMetadata meta = metadata.getColumnMetadata(i);
			if (meta.type == ColumnType.SYMBOL && meta.sameAs == null) {
				SymbolTable tab = getSymbolTable(meta.sameAs);
				symbolTableMap.put(meta.name, tab);
				columnMetadata[i].symbolTable = tab;
			}
		}
	}

	private void configurePartitions(Tx tx) throws JournalException {
		File[] files = getLocation().listFiles(f -> f.isDirectory() && !f.getName().startsWith(JournalConfiguration.TEMP_DIRECTORY_PREFIX));

		int partitionIndex = 0;
		if (files != null && tx.journalMaxRowID > 0) {
			Arrays.sort(files);
			for (File f : files) {

				if (partitionIndex > Rows.toPartitionIndex(tx.journalMaxRowID)) {
					break;
				}

				Partition<T> partition = null;

				if (partitionIndex < partitions.size()) {
					partition = partitions.get(partitionIndex);
				}

				long txLimit = Journal.TX_LIMIT_EVAL;
				long[] indexTxAddresses = null;
				if (partitionIndex == Rows.toPartitionIndex(tx.journalMaxRowID)) {
					txLimit = Rows.toLocalRowID(tx.journalMaxRowID);
					indexTxAddresses = tx.indexPointers;
				}

				Interval interval = Dates.intervalForDirName(f.getName(), getMetadata().getPartitionType());
				if (partition != null) {
					if (partition.getInterval() == null || partition.getInterval().equals(interval)) {
						partition.applyTx(txLimit, indexTxAddresses);
						partitionIndex++;
					}
					else {
						if (partition.isOpen()) {
							partition.close();
						}
						partitions.remove(partitionIndex);
					}
				}
				else {
					partitions.add(new Partition<>(this, interval, partitionIndex, txLimit, indexTxAddresses));
					partitionIndex++;
				}
			}
		}
		configureIrregularPartition(tx);
	}

	private void configureIrregularPartition(Tx tx) throws JournalException {
		// if journal is under intense write activity in another process
		// lag partition can keep changing
		// so we will be trying to pin lag partition
		while (true) {
			String lagPartitionName = tx != null ? tx.lagName : null;
			if (lagPartitionName != null && (irregularPartition == null || !lagPartitionName.equals(irregularPartition.getName()))) {
				// new lag partition
				// try to lock be trying to pin lag partition
				File lagLocation = new File(getLocation(), lagPartitionName);
				LOGGER.trace("Attempting to attach partition %s to %s", lagLocation.getName(), this);
				Lock lock = LockManager.lockShared(lagLocation);

				try {
					// if our lock has been successful
					// continue with replacing lag
					if (lock != null && lock.isValid()) {
						LOGGER.trace("Lock successful for %s", lagLocation.getName());
						Partition<T> temp = createTempPartition(lagPartitionName);
						temp.applyTx(tx.lagSize, tx.lagIndexPointers);
						setIrregularPartition(temp);
						// exit out of while loop
						break;
					}
					else {
						LOGGER.debug("Lock successful for %s", lagLocation.getName());
					}
				}
				finally {
					LockManager.release(lock);
				}
			}
			else if (lagPartitionName != null && irregularPartition != null && lagPartitionName.equals(irregularPartition.getName())) {
				irregularPartition.applyTx(tx.lagSize, tx.lagIndexPointers);
				break;
			}
			else if (lagPartitionName == null && irregularPartition != null) {
				removeIrregularPartitionInternal();
				break;
			}
			else {
				// there is no lag partition, exit.
				break;
			}
		}
	}

	/////////////////////////////////////////////////////////////////

	public static class ColumnMetadata {
		public SymbolTable symbolTable;
		public JournalMetadata.ColumnMetadata meta;
	}
}
