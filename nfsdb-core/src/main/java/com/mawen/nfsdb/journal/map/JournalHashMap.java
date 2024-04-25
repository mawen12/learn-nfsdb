package com.mawen.nfsdb.journal.map;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.utils.Unsafe;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class JournalHashMap<T> implements JournalMap<T> {

	private final HashMap<String, T> map;
	private final Set<String> invalidKeyCache;
	private final Journal<T> journal;
	private final String column;
	private final long columnOffset;
	private final JournalMapFilter<T> filter;
	private boolean eager = false;


	public JournalHashMap(Journal<T> journal) {
		this(journal, journal.getMetadata().getKey());
	}

	public JournalHashMap(Journal<T> journal, String column) {
		this(journal, column, null);
	}

	public JournalHashMap(Journal<T> journal, String column, JournalMapFilter<T> filter) {
		this.journal = journal;
		this.column = column;
		this.filter = filter;
		this.map = new HashMap<>(journal.getSymbolTable(column).size());
		this.columnOffset = journal.getMetadata().getColumnMetadata(column).offset;
		this.invalidKeyCache = new HashSet<>();
	}

	@Override
	public JournalMap<T> eager() throws JournalException {
		for (T t : journal.query().head().withSymValues(column).asResultSet()) {
			if (filter == null || filter.accept(t)) {
				map.put((String) Unsafe.getUnsafe().getObject(t, columnOffset), t);
			}
		}
		eager = true;
		return this;
	}

	@Override
	public T get(String key) {
		T result = map.get(key);
		if (result == null && !eager) {
			if (!invalidKeyCache.contains(key)) {
				try {
					result = journal.query().head().withSymValues(column).asResultSet().readFirst();
				}
				catch (JournalException e) {
					throw new JournalRuntimeException(e);
				}

				if (filter != null && result != null && !filter.accept(result)) {
					result = null;
				}

				if (result != null) {
					map.put((String) Unsafe.getUnsafe().getObject(result, columnOffset), result);
				}
				else {
					invalidKeyCache.add(key);
				}
			}
		}

		return result;
	}

	@Override
	public Collection<T> values() {
		return map.values();
	}

	@Override
	public boolean refresh() throws JournalException {
		if (journal.refresh()) {
			map.clear();
			if (eager) {
				eager();
			}
			return true;
		}
		return false;
	}

	@Override
	public Set<String> keys() {
		return map.keySet();
	}

	@Override
	public String getColumn() {
		return column;
	}

	@Override
	public int size() {
		return map.size();
	}
}
