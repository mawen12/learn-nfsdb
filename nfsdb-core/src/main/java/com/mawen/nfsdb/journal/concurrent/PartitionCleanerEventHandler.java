package com.mawen.nfsdb.journal.concurrent;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.JournalWriter;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.tx.TxLog;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class PartitionCleanerEventHandler implements EventHandler<PartitionCleanerEvent>, LifecycleAware {
	private final JournalWriter writer;
	private TxLog txLog;

	public PartitionCleanerEventHandler(JournalWriter writer) {
		this.writer = writer;
	}

	@Override
	public void onEvent(PartitionCleanerEvent partitionCleanerEvent, long sequence, boolean endOfBatch) throws Exception {
		if (endOfBatch) {
			writer.purgeUnusedTempPartitions(txLog);
		}
	}

	@Override
	public void onStart() {
		try {
			this.txLog = new TxLog(writer.getLocation(), JournalMode.READ);
		}
		catch (JournalException e) {
			throw new JournalRuntimeException(e);
		}
	}

	@Override
	public void onShutdown() {
		if (txLog != null) {
			txLog.close();
		}
	}
}
