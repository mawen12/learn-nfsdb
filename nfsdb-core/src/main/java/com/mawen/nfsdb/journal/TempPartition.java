package com.mawen.nfsdb.journal;

import java.io.File;
import java.io.IOException;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.locks.Lock;
import com.mawen.nfsdb.journal.locks.LockManager;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class TempPartition<T> extends Partition<T> {

	private final Lock lock;

	public TempPartition(Journal<T> journal, Interval interval, int partitionIndex, String name) throws JournalException {
		super(journal, interval, partitionIndex, Journal.TX_LIMIT_EVAL, null);
		setPartitionDir(new File(journal.getLocation(), name), null);
		this.lock = LockManager.lockShared(getPartitionDir());
	}

	@Override
	public void close() {
		super.close();
		LockManager.release(lock);
	}
}
