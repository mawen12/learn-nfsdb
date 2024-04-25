package com.mawen.nfsdb.journal.factory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mawen.nfsdb.journal.concurrent.NamedDaemonThreadFactory;
import com.mawen.nfsdb.journal.concurrent.TimerCache;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class JournalPool implements Closeable {

	private final ArrayBlockingQueue<JournalCachingFactory> pool;
	private final ExecutorService service = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("pool-release-thread", true));
	private final AtomicBoolean running = new AtomicBoolean(true);

	public JournalPool(JournalConfiguration configuration, int capacity) {
		this.pool = new ArrayBlockingQueue<>(capacity, true);

		TimerCache timerCache = new TimerCache().start();
		for (int i = 0; i < capacity; i++) {
			assert pool.offer(new JournalCachingFactory(configuration, timerCache, this));
		}
	}

	public JournalCachingFactory get() throws InterruptedException {
		if (running.get()) {
			JournalCachingFactory factory = pool.take();
			factory.refresh();
			return factory;
		}
		else {
			throw new InterruptedException("Journal pool has been closed");
		}
	}

	@Override
	public void close() throws IOException {
		if (running.compareAndExchange(true, false)) {
			for (JournalCachingFactory factory : pool) {
				factory.clearPool();
				factory.close();
			}
		}
	}

	void release(final JournalCachingFactory factory) {
		service.submit(() -> {
			while (true) {
				factory.expireOpenFiles();
				if (pool.offer(factory)) {
					break;
				}
			}
		});
	}
}
