package com.mawen.nfsdb.journal.query.api;

import com.mawen.nfsdb.journal.Journal;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class QueryImpl<T> implements Query<T> {

	private final Journal<T> journal;
	private final ;

	@Override
	public QueryAll<T> all() {
		return null;
	}

	@Override
	public QueryHead<T> head() {
		return null;
	}

	@Override
	public Journal<T> getJournal() {
		return null;
	}
}
