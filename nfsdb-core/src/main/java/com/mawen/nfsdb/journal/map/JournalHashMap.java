package com.mawen.nfsdb.journal.map;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class JournalHashMap<T> extends JournalMap<T> {

	private final HashMap<String, T> map;
	private final Set<String> invalidKeyCache;
	private final Journal<T> journal;
	private final String column;
	private final long columnOffset;
	private final JournalMapFilter<T> filter;
	private boolean eager = false;


	@Override
	public JournalMap<T> eager() throws JournalException {

	}

	@Override
	public T get(String key) {

	}

	@Override
	public Collection<T> values() {
		return map.values();
	}

	@Override
	public boolean refresh() throws JournalException {

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
