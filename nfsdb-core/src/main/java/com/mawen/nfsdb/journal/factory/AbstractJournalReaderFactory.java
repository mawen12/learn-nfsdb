package com.mawen.nfsdb.journal.factory;

import java.io.Closeable;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.JournalKey;
import com.mawen.nfsdb.journal.concurrent.TimerCache;
import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public abstract class AbstractJournalReaderFactory implements JournalReaderFactory, Closeable {

	private final TimerCache timerCache;
	private final JournalConfiguration configuration;

	@Override
	public void close() {
	}

	@Override
	public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
		return new Journal<>(key, configuration.getMetadata(key), timerCache);
	}

	@Override
	public <T> Journal<T> reader(Class<T> clazz) throws JournalException {
		return reader(new JournalKey<>(clazz));
	}

	@Override
	public <T> Journal<T> reader(Class<T> clazz, String location) throws JournalException {
		return reader(new JournalKey<>(clazz, location));
	}

	@Override
	public JournalConfiguration getConfiguration() {
		return configuration;
	}

	protected AbstractJournalReaderFactory(JournalConfiguration configuration) {
		this(configuration, new TimerCache().start());
	}

	protected AbstractJournalReaderFactory(JournalConfiguration configuration, TimerCache timerCache) {
		this.configuration = configuration;
		this.timerCache = timerCache;
	}

	protected TimerCache getTimerCache() {
		return timerCache;
	}
}
