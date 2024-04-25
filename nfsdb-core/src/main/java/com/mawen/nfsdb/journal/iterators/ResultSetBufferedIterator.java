package com.mawen.nfsdb.journal.iterators;

import java.util.Iterator;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.ResultSet;
import com.mawen.nfsdb.journal.exceptions.JournalImmutableIteratorException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class ResultSetBufferedIterator<T> implements JournalIterator<T> {

	private final ResultSet<T> rs;
	private final T obj;
	private int counter = 0;

	public ResultSetBufferedIterator(ResultSet<T> rs) {
		this.rs = rs;
		this.obj = rs.getJournal().newObject();
	}

	@Override
	public boolean hasNext() {
		return counter < rs.size();
	}

	@Override
	public T next() {
		try {
			rs.getJournal().clearObject(obj);
			rs.read(counter++, obj);
			return obj;
		}
		catch (Exception e) {
			throw new JournalRuntimeException("Journal exception", e);
		}
	}

	@Override
	public void remove() {
		throw new JournalImmutableIteratorException();
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}

	@Override
	public Journal<T> getJournal() {
		return rs.getJournal();
	}
}
