package com.mawen.nfsdb.journal.concurrent;

import com.lmax.disruptor.EventFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class PartitionCleanerEvent {

	public static final EventFactory<PartitionCleanerEvent> EVENT_FACTORY = PartitionCleanerEvent::new;
}
