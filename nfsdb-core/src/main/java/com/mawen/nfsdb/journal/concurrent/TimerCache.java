package com.mawen.nfsdb.journal.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class TimerCache {
	private final long updateFrequency;
	private final ExecutorService service = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("jj-timer-cache", true));
	private volatile long millis = System.currentTimeMillis();

	public TimerCache() {
		this.updateFrequency = TimeUnit.SECONDS.toNanos(1);
	}

	public TimerCache start() {
		service.submit(() -> {
			millis = System.currentTimeMillis();
			LockSupport.parkNanos(updateFrequency);
		});
		return this;
	}

	public long getMillis() {
		return millis;
	}
}
