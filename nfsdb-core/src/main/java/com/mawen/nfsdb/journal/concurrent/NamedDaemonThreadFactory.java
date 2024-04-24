package com.mawen.nfsdb.journal.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class NamedDaemonThreadFactory implements ThreadFactory {
	private final String name;
	private final boolean daemon;
	private int count = 0;

	public NamedDaemonThreadFactory(String name, boolean daemon) {
		this.name = name;
		this.daemon = daemon;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread thread = new Thread(r, name + "-" + count++);
		thread.setDaemon(daemon);
		return thread;
	}
}
