package com.mawen.nfsdb.journal.concurrent;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.mawen.nfsdb.journal.tx.TxLog;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class PartitionCleanerEventHandler implements EventHandler<PartitionCleanerEvent>, LifecycleAware {
	private final JournalWriter writer;
	private TxLog txLog;

	@Override
	public void onEvent(PartitionCleanerEvent partitionCleanerEvent, long l, boolean b) throws Exception {

	}

	@Override
	public void onStart() {

	}

	@Override
	public void onShutdown() {

	}
}
