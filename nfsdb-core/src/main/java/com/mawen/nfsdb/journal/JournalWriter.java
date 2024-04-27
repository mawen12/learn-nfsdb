package com.mawen.nfsdb.journal;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.mawen.nfsdb.journal.column.FixedWidthColumn;
import com.mawen.nfsdb.journal.column.SymbolTable;
import com.mawen.nfsdb.journal.concurrent.PartitionCleaner;
import com.mawen.nfsdb.journal.concurrent.TimerCache;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.iterators.MergingIterator;
import com.mawen.nfsdb.journal.iterators.ParallelIterator;
import com.mawen.nfsdb.journal.locks.Lock;
import com.mawen.nfsdb.journal.locks.LockManager;
import com.mawen.nfsdb.journal.logging.Logger;
import com.mawen.nfsdb.journal.tx.Tx;
import com.mawen.nfsdb.journal.tx.TxFuture;
import com.mawen.nfsdb.journal.tx.TxListener;
import com.mawen.nfsdb.journal.tx.TxLog;
import com.mawen.nfsdb.journal.utils.Dates;
import com.mawen.nfsdb.journal.utils.Files;
import com.mawen.nfsdb.journal.utils.Rows;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class JournalWriter<T> extends Journal<T> {
	private static final Logger LOGGER = Logger.getLogger(JournalWriter.class);

	private final long lagMillis;
	private final long lagSwellMillis;
	private Lock writeLock;
	private TxListener txListener;
	private boolean txActive = false;
	private int txPartitionIndex = -1;
	private long hardMaxTimestamp = 0;
	private PartitionCleaner partitionCleaner;
	private boolean autoCommit = true;
	// irregular partition related
	private boolean doDiscard = true;
	private boolean doJournal = true;

	public JournalWriter(JournalMetadata<T> metadata, JournalKey<T> key, TimerCache timerCache) throws JournalException {
		super(key, metadata, timerCache);
		this.lagMillis = TimeUnit.HOURS.toMillis(getMetadata().getLagHours());
		this.lagSwellMillis = lagMillis * 3;
	}

	@Override
	public void close() {
		if (partitionCleaner != null) {
			partitionCleaner.halt();
			partitionCleaner = null;
		}

		try {
			if (isAutoCommit()) {
				commit();
				purgeUnusedTempPartitions(txLog);
			}
			super.close();
			if (writeLock != null) {
				LockManager.release(writeLock);
				writeLock = null;
			}
		}
		catch (JournalException e) {
			throw new JournalRuntimeException(e);
		}
	}

	public TxFuture commitAsync() throws JournalException {
		TxFuture future = null;
		if (txActive) {
			commit(Tx.TX_NORMAL);
			if (txListener != null) {
				future = txListener.notifyAsync();
			}
			txActive = false;
		}
		return future;
	}

	public boolean commitAndAwait(long timeout, TimeUnit unit) throws JournalException {
		boolean result = true;
		if (txActive) {
			commit(Tx.TX_NORMAL);
			if (txListener != null) {
				result = txListener.notifySync(timeout, unit);
			}
			txActive = false;
		}
		return result;
	}

	public void rollback() throws JournalException {
		if (txActive) {
			Tx tx = txLog.get();

			// partitions need to be dealt with first to make sure new lag is assigned a correct parttitionIndex
			rollbackPartitions(tx);

			Partition<T> lag = getIrregularPartition();
			if (tx.lagName != null && tx.lagName.length() > 0 && (lag == null || !tx.lagName.equals(lag.getName()))) {
				TempPartition<T> newLag = createTempPartition(tx.lagName);
				setIrregularPartition(newLag);
				newLag.applyTx(tx.lagSize, tx.lagIndexPointers);
			}
			else if (lag != null && tx.lagName == null) {
				removeIrregularPartition();
			}
			else if (lag != null) {
				lag.truncate(tx.lagSize);
			}

			if (tx.symbolTableSizes.length == 0) {
				for (int i = 0; i < getSymbolTableCount(); i++) {
					getSymbolTable(i).truncate();
				}
			}
			else {
				for (int i = 0; i < getSymbolTableCount(); i++) {
					getSymbolTable(i).truncate(tx.symbolTableSizes[i]);
				}
			}
			hardMaxTimestamp = 0;
			txActive = false;
		}
	}

	public void setTxListener(TxListener txListener) {
		this.txListener = txListener;
	}

	public Partition<T> getPartitionForTimestamp(long timestamp) {
		for (int i = 0, partitionSize = partitions.size(); i < partitionSize; i++) {
			Partition<T> result = partitions.get(i);
			if (result.getInterval() == null || result.getInterval().contains(timestamp)) {
				return result.access();
			}
		}

		if (partitions.get(0).getInterval().isAfter(timestamp)) {
			return partitions.get(0).access();
		}
		else {
			return partitions.get(nonLagPartitionCount() - 1).access();
		}
	}

	/**
	 * Opens existing lag partition if it exists or creates new one if parent journal is configured to
	 * have lag partitions
	 *
	 * @return Lag partition instance.
	 * @throws JournalException
	 */
	public Partition<T> openOrCreateLagPartition() throws JournalException {
		Partition<T> result = getIrregularPartition();
		if (result == null) {
			result = createTempPartition();
			setIrregularPartition(result);
		}
		return result.open();
	}

	public void removeIrregularPartition() {
		beginTx();
		removeIrregularPartitionInternal();
	}

	public void beginTx() {
		if (!txActive) {
			this.txActive = true;
			this.txPartitionIndex = nonLagPartitionCount() - 1;
		}
	}

	public void purgeUnusedTempPartitions(TxLog txLog) throws JournalException {
		if (getMode() == JournalMode.APPEND) {
			final String lagPartitionName = hasIrregularPartition() ? getIrregularPartition().getName() : null;
			final String txLagName = txLog != null ? txLog.get().lagName : null;

			File[] files = getLocation().listFiles(f -> f.isDirectory() && f.getName().startsWith(JournalConfiguration.TEMP_DIRECTORY_PREFIX)
					&& (lagPartitionName == null || !lagPartitionName.equals(f.getName()))
					&& (txLagName == null || !txLagName.equals(f.getName())));

			Arrays.sort(files);

			for (File file : files) {
				// get exclusive lock
				Lock lock = LockManager.lockExclusive(file);
				try {
					if (lock != null && lock.isValid()) {
						LOGGER.trace("Purging: %s", file);
						if (!Files.delete(file)) {
							LOGGER.info("Could not purge: %s", file);
						}
					}
					else {
						LOGGER.trace("Partition in use: %s", file);
					}
				}
				finally {
					LockManager.release(lock);
				}
			}
		}
		else {
			throw new JournalRuntimeException("Cannot purge temp partitions in read-only mode");
		}
	}

	public void rebuildIndexes() throws JournalException {
		for (int i = 0; i < getPartitionCount(); i++) {
			getPartition(i, true).rebuildIndexes();
		}
	}

	public void compact() throws JournalException {
		for (int i = 0; i < getPartitionCount(); i++) {
			getPartition(i, true).compact();
		}
	}

	public void truncate() throws JournalException {
		beginTx();

		for (int i = 0; i < getPartitionCount(); i++) {
			Partition<T> partition = getPartition(i, true);
			partition.truncate(0);
			partition.close();
			Files.deleteOrException(partition.getPartitionDir());
		}

		closePartitions();

		for (int i = 0; i < getSymbolTableCount(); i++) {
			getSymbolTable(i).truncate();
		}

		hardMaxTimestamp = 0;
		commit();
	}

	public void commit() throws JournalException {
		if (txActive) {
			commit(Tx.TX_NORMAL);
			expireOpenFiles();
			if (txListener != null) {
				txListener.notifyAsyncNoWait();
			}
			txActive = false;
		}
	}

	/**
	 * Delete entire Journal.
	 *
	 * @throws JournalException if the journal is open (must be closed)
	 */
	public void delete() throws JournalException {
		if (isOpen()) {
			throw new JournalException("Cannot delete open journal: %s", this);
		}
		Files.deleteOrException(getLocation());
	}

	public void append(Journal<T> journal) throws JournalException {
		try (ParallelIterator<T> iterator = journal.parallelIterator()) {
			for (T obj : iterator) {
				append(obj);
			}
		}
	}

	public void append(ResultSet<T> resultSet) throws JournalException {
		if (isCompatible(resultSet.getJournal())) {
			for (T obj : resultSet.bufferedIterator()) {
				this.append(obj);
			}
		}
		else {
			throw new JournalException("%s is incompatible with %s", this, resultSet.getJournal());
		}
	}


	public final void append(T... objects) throws JournalException {
		for (T o : objects) {
			append(o);
		}
	}

	/**
	 * Add objects to the end of journal
	 *
	 * @param objects objects to add
	 * @throws JournalException if there is an error
	 */
	public void append(Iterable<T> objects) throws JournalException {
		for (T o : objects) {
			append(o);
		}
	}

	/**
	 * Add an object to the end of the journal
	 *
	 * @param obj the object to add
	 * @throws JournalException if there is an error
	 */
	public void append(T obj) throws JournalException {

		if (obj == null) {
			throw new JournalException("Cannot append NULL to %s", this);
		}

		beginTx();

		if (getTimestampOffset() != -1) {
			long timestamp = getTimestamp(obj);
			if (getKey().isOrdered() && timestamp < getImmutableMaxTimestamp()) {
				throw new JournalException("Cannot insert records out of order. maxHardTimestamp=%d (%s), timestamp=%d (%s): (%s)",
						hardMaxTimestamp, Dates.toString(hardMaxTimestamp), timestamp, Dates.toString(timestamp), this);
			}
			getAppendPartition(timestamp).append(obj);

			if (timestamp > hardMaxTimestamp) {
				hardMaxTimestamp = timestamp;
			}
		}
		else {
			getAppendPartition().append(obj);
		}
	}

	public boolean isTxActive() {
		return txActive;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	public void purgeTempPartitions() {
		partitionCleaner.purge();
	}

	@Override
	public Journal<T> setReadColumns(String... readColumns) {
		throw new JournalRuntimeException("Cannot limit columns in read/write flow model");
	}

	@Override
	public JournalMode getMode() {
		return JournalMode.APPEND;
	}

	public void appendIrregular(List<T> data) throws JournalException {

		if (lagMillis == 0) {
			throw new JournalException("This journal is not configured to have lag partition");
		}

		beginTx();

		if (data == null || data.size() == 0) {
			return;
		}

		long dataMaxTimestamp = getTimestamp(data.get(data.size() - 1));
		long hard = getImmutableMaxTimestamp();

		if (dataMaxTimestamp < hard) {
			return;
		}

		final Partition<T> lagPartition = openOrCreateLagPartition();
		this.doDiscard = true;
		this.doJournal = true;

		long dataMinTimestamp = getTimestamp(data.get(0));
		long lagMaxTimestamp = getMaxTimestamp();
		long lagMinTimestamp = lagPartition.size() == 0L ? 0 : getTimestamp(lagPartition.read(0));
		long soft = Math.max(dataMaxTimestamp, lagMaxTimestamp) - lagMillis;

		if (dataMinTimestamp > dataMaxTimestamp) {
			// this could be as simple as just appending data to lag
			// the only complication is that after adding records to lag it could swell beyond
			// the allocated "lagSwellTimestamp"
			// we should check if this is going to happen and optimize copying of data

			long lagSizeMillis;
			if (hard > 0L) {
				lagSizeMillis = dataMaxTimestamp - hard;
			}
			else if (lagMinTimestamp > 0L) {
				lagSizeMillis = dataMaxTimestamp - lagMinTimestamp;
			}
			else {
				lagSizeMillis = 0L;
			}

			if (lagSizeMillis > lagSwellMillis) {
				// data would be too big and would stretch outside of swell timestamp
				// this is when lag partition should be split, but it is still a straight split without re-order

				Partition<T> tempPartition = createTempPartition().open();
				splitAppend(lagPartition.bufferedIterator(), hard, soft, tempPartition);
				splitAppend(data.iterator(), hard, soft, tempPartition);
				replaceIrregularPartition(tempPartition);
			}
			else {
				// simplest case, just append to log
				lagPartition.append(data.iterator());
			}
		}
		else {
			Partition<T> tempPartition = createTempPartition().open();
			if (dataMinTimestamp > lagMinTimestamp && dataMaxTimestamp < lagMaxTimestamp) {
				// overlap scenario 1: data is fully inside of lag

				// calc boundaries of lag that intersects with data
				long lagMid1 = lagPartition.indexOf(dataMinTimestamp, BinarySearch.SearchType.LESS_OR_EQUAL);
				long lagMid2 = lagPartition.indexOf(dataMaxTimestamp, BinarySearch.SearchType.GREATER_OR_EQUAL);

				// copy part of lag above data
				splitAppend(lagPartition.bufferedIterator(0, lagMid1), hard, soft, tempPartition);

				// merge lag with data and copy result to temp partition
				splitAppendMerge(data.iterator(), lagPartition.iterator(lagMid1 + 1, lagMid2 - 1), hard, soft, tempPartition);

				// copy part of lag below data
				splitAppend(lagPartition.bufferedIterator(lagMid2, lagPartition.size() - 1), hard, soft, tempPartition);
			}
			else if (dataMinTimestamp <= lagMinTimestamp && dataMaxTimestamp < lagMaxTimestamp) {
				// overlap scenario 2: data sits directly above lag

				splitAppend(data.iterator(), hard, soft, tempPartition);
				splitAppend(lagPartition.bufferedIterator(), hard, soft, tempPartition);
			}
			else if (dataMinTimestamp < lagMinTimestamp && dataMaxTimestamp <= lagMaxTimestamp) {
				// overlap scenario 3: bottom part of data overlaps top part of lag

				// calc overlap line
				long split = lagPartition.indexOf(dataMaxTimestamp, BinarySearch.SearchType.GREATER_OR_EQUAL);

				// merge lag with data and copy result to temp partition
				splitAppendMerge(data.iterator(), lagPartition.iterator(0, split - 1), hard, soft, tempPartition);

				// copy part of lag below data
				splitAppend(lagPartition.bufferedIterator(split, lagPartition.size() - 1), hard, soft, tempPartition);
			}
			else if (dataMinTimestamp > lagMinTimestamp && dataMaxTimestamp >= lagMaxTimestamp) {
				// overlap scenario 4: top part of data overlaps with bottom part of lag

				long split = lagPartition.indexOf(dataMinTimestamp, BinarySearch.SearchType.LESS_OR_EQUAL);

				// copy part of lag above overlap
				splitAppend(lagPartition.bufferedIterator(0, split), hard, soft, tempPartition);

				// merge lag with data and copy result to temp partition
				splitAppendMerge(data.iterator(), lagPartition.iterator(split + 1, lagPartition.size() - 1), hard, soft, tempPartition);
			}
			else if (dataMinTimestamp <= lagMinTimestamp && dataMinTimestamp >= lagMaxTimestamp) {
				// overlap scenario 5: lag is fully inside of data

				// merge lag with data and copy result to temp partition
				splitAppendMerge(data.iterator(), lagPartition.iterator(0, lagPartition.size() - 1), hard, soft, tempPartition);
			}
			else {
				throw new JournalRuntimeException("Unsupported overlap type: lag min/max: [%s/%s] data min/max: [%s/%s]",
						Dates.toString(lagMinTimestamp), Dates.toString(lagMaxTimestamp),
						Dates.toString(dataMinTimestamp), Dates.toString(dataMaxTimestamp));
			}

			replaceIrregularPartition(tempPartition);
		}
	}

	/////////////////////////////////////////////////////////////////

	@Override
	protected void configure() throws JournalException {
		writeLock = LockManager.lockExclusive(getLocation());
		if (writeLock == null || !writeLock.isValid()) {
			close();
			throw new JournalException("Journal is already open for APPEND at %s", getLocation());
		}
		if (txLog.isEmpty()) {
			commit(Tx.TX_NORMAL);
		}
		Tx tx = txLog.get();

		File meta = new File(getLocation(), JournalConfiguration.JOURNAL_META_FILE);
		if (!meta.exists()) {
			Files.writeStringToFile(meta, getMetadata().toString());
		}

		super.configure();

		beginTx();
		rollback();
		rollbackPartitionDirs();

		if (tx.journalMaxRowID > 0 && getPartitionCount() <= Rows.toPartitionIndex(tx.journalMaxRowID)) {
			beginTx();
			commit();
		}
		if (getMetadata().getLagHours() != -1) {
			this.partitionCleaner = new PartitionCleaner(this, getLocation().getName());
			this.partitionCleaner.start();
		}
	}

	public Partition<T> getAppendPartition(long timestamp) throws JournalException {
		int sz = nonLagPartitionCount();
		if (sz > 0) {
			Partition<T> result = partitions.get(sz - 1).access();
			if (result.getInterval() == null || result.getInterval().contains(timestamp)) {
				return result.open();
			}
			else if (result.getInterval().isBefore(timestamp)) {
				return createPartition(Dates.intervalForDate(timestamp,getMetadata().getPartitionType()), sz);
			}
			else {
				throw new JournalException("%s cannot be appended to %s", Dates.toString(timestamp), this);
			}
		}
		else {
			return createPartition(Dates.intervalForDate(timestamp, getMetadata().getPartitionType()), 0);
		}
	}

	public Partition<T> createPartition(Interval interval, int partitionIndex) {
		Partition<T> result = new Partition<>(this, interval, partitionIndex, Journal.TX_LIMIT_EVAL, null);
		partitions.add(result);
		return result;
	}

	public long getImmutableMaxTimestamp() throws JournalException {
		if (hardMaxTimestamp == 0) {
			if (nonLagPartitionCount() == 0) {
				return 0;
			}

			FixedWidthColumn column = lastNonEmptyNonLag().getTimestampColumn();
			if (column.size() > 0) {
				hardMaxTimestamp = column.getLong(column.size() - 1);
			}
			else {
				return 0;
			}
		}

		return hardMaxTimestamp;
	}

	public Partition<T> lastNonEmptyNonLag() throws JournalException {
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

	/////////////////////////////////////////////////////////////////

	Partition<T> createTempPartition() throws JournalException {
		return createTempPartition(JournalConfiguration.TEMP_DIRECTORY_PREFIX + "." + System.currentTimeMillis() + "." + UUID.randomUUID());
	}

	Partition<T> getAppendPartition() throws JournalException {
		if (nonLagPartitionCount() > 0) {
			return getPartition(nonLagPartitionCount() - 1, true);
		}
		else {
			if (getMetadata().getPartitionType() != PartitionType.NONE) {
				throw new JournalException("getAppendPartition() without timestamp on partitioned journal: %s", this);
			}
			return createPartition(null, 0);
		}
	}

	/////////////////////////////////////////////////////////////////


	private void commit(byte command) throws JournalException {
		Partition<T> partition = lagNonEmptyNonLag();
		Partition<T> lag = getIrregularPartition();

		Tx tx = new Tx();
		tx.command = command;
		tx.journalMaxRowID = partition == null ? 0 : Rows.toRowID(partition.getPartitionIndex(), partition.size());
		tx.lastPartitionTimestamp = partition == null || partition.getInterval() == null ? 0 : partition.getInterval().getStartMillis();
		tx.lagSize = lag == null ? 0 : lag.open().size();
		tx.lagName = lag == null ? null : lag.getName();
		tx.symbolTableSizes = new int[getSymbolTableCount()];
		tx.symbolTableIndexPointers = new long[getSymbolTableCount()];
		for (int i = 0; i < getSymbolTableCount(); i++) {
			SymbolTable tab = getSymbolTable(i);
			tab.commit();
			tx.symbolTableSizes[i] = tab.size();
			tx.symbolTableIndexPointers[i] = tab.getIndexTxAddress();
		}
		tx.indexPointers = new long[getMetadata().getColumnCount()];

		for (int i = Math.max(txPartitionIndex, 0); i < nonLagPartitionCount(); i++) {
			getPartition(i, true).commit();
		}

		if (partition != null) {
			partition.getIndexPointers(tx.indexPointers);
		}

		tx.lagIndexPointers = new long[getMetadata().getColumnCount()];
		if (lag != null) {
			lag.commit();
			lag.getIndexPointers(tx.lagIndexPointers);
		}

		txLog.create(tx);
	}

	private void rollbackPartitionDirs() throws JournalException {
		File[] files = getLocation().listFiles(f -> f.isDirectory() && !f.getName().startsWith(JournalConfiguration.TEMP_DIRECTORY_PREFIX));

		if (files != null) {
			Arrays.sort(files);
			for (int i = getPartitionCount(); i < files.length; i++) {
				Files.deleteOrException(files[i]);
			}
		}
	}

	private void rollbackPartitions(Tx tx) throws JournalException {
		int partitionIndex = Rows.toPartitionIndex(tx.journalMaxRowID);
		for (Iterator<Partition<T>> it = partitions.iterator(); it.hasNext(); ) {
			Partition<T> partition = it.next();
			if (partition.getPartitionIndex() == partitionIndex) {
				partition.open();
				partition.truncate(Rows.toLocalRowID(tx.journalMaxRowID));
			}
			else if (partition.getPartitionIndex() > partitionIndex) {
				it.remove();
				partition.close();
				Files.deleteOrException(partition.getPartitionDir());
			}
		}
	}

	private void splitAppendMerge(Iterator<T> a, Iterator<T> b, long hard, long soft, Partition<T> temp) throws JournalException {
		splitAppend(new MergingIterator<>(a, b, getTimestampComparator()), hard, soft, temp);
	}

	private void replaceIrregularPartition(Partition<T> temp) {
		setIrregularPartition(temp);
		purgeTempPartitions();
	}

	private void splitAppend(Iterator<T> it, long hard, long soft, Partition<T> partition) throws JournalException {
		while (it.hasNext()) {
			T obj = it.next();
			if (doDiscard && getTimestamp(obj) < hard) {
				// discard
				continue;
			}
			else if (doDiscard) {
				doDiscard = false;
			}

			if (doJournal && getTimestamp(obj) < soft) {
				append(obj);
				continue;
			}
			else if (doJournal) {
				doJournal = false;
			}

			partition.append(obj);
		}
	}
}
