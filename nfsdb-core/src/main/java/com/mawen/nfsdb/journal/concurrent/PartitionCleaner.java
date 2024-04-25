package com.mawen.nfsdb.journal.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.mawen.nfsdb.journal.JournalWriter;
import com.mawen.nfsdb.journal.logging.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class PartitionCleaner {
	private static final Logger LOGGER = Logger.getLogger(PartitionCleaner.class);

	private final ExecutorService executor;
	private final RingBuffer<PartitionCleanerEvent> ringBuffer = RingBuffer.createSingleProducer(PartitionCleanerEvent.EVENT_FACTORY, 32, new BlockingWaitStrategy());
	private final BatchEventProcessor<PartitionCleanerEvent> batchEventProcessor;
	private boolean started = false;

	public PartitionCleaner(JournalWriter writer, String name) {
		this.executor = Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("jj-cleaner-" + name, true));
		this.batchEventProcessor = new BatchEventProcessor<>(ringBuffer, ringBuffer.newBarrier(), new PartitionCleanerEventHandler(writer));
		ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
	}

	public void start() {
		started = true;
		executor.submit(batchEventProcessor);
	}

	public void purge() {
		ringBuffer.publish(ringBuffer.next());
	}

	public void halt() {
		executor.shutdown();

		while (started && !batchEventProcessor.isRunning()) {
			Thread.yield();
		}

		started = false;

		do {
			batchEventProcessor.halt();
		} while (batchEventProcessor.isRunning());

		try {
			executor.awaitTermination(30, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			LOGGER.info("Partition cleaner shutdown, but thread is still running");
		}
	}
}
