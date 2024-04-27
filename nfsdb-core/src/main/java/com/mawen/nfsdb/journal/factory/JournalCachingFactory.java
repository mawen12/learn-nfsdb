package com.mawen.nfsdb.journal.factory;

import java.util.HashMap;
import java.util.Map;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.JournalKey;
import com.mawen.nfsdb.journal.concurrent.TimerCache;
import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class JournalCachingFactory extends AbstractJournalReaderFactory implements JournalClosingListener {

	private final Map<JournalKey, Journal> journals = new HashMap<>();
	private JournalPool pool;

	public JournalCachingFactory(JournalConfiguration configuration) {
		this(configuration, new TimerCache().start());
	}

	public JournalCachingFactory(JournalConfiguration configuration, TimerCache timerCache) {
		this(configuration, timerCache, null);
	}

	public JournalCachingFactory(JournalConfiguration configuration, TimerCache timerCache, JournalPool pool) {
		super(configuration, timerCache);
		this.pool = pool;
	}

	@Override
	public void close() {
		if (pool != null) {
			pool.release(this);
		}
		else {
			for (Journal journal : journals.values()) {
				journal.setCloseListener(null);
				journal.close();
			}
			journals.clear();
		}

	}

	@Override
	public boolean closing(Journal journal) {
		return false;
	}

	@Override
	public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
		Journal<T> result = journals.get(key);
		if (result == null) {
			result = super.reader(key);
			result.setCloseListener(this);
			journals.put(key, result);
		}
		return result;
	}

	public void refresh() throws JournalException {
		for (Journal journal : journals.values()) {
			journal.refresh();
		}
	}

	void clearPool() {
		this.pool = null;
	}

	void expireOpenFiles() {
		for (Journal journal : journals.values()) {
			journal.expireOpenFiles();
		}
	}
}
