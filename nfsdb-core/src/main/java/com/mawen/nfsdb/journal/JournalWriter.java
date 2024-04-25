package com.mawen.nfsdb.journal;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.mawen.nfsdb.journal.concurrent.PartitionCleaner;
import com.mawen.nfsdb.journal.concurrent.TimerCache;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.locks.Lock;
import com.mawen.nfsdb.journal.logging.Logger;
import com.mawen.nfsdb.journal.tx.TxListener;

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
	public void close() throws IOException {

	}
}
