package com.mawen.nfsdb.journal.query.spi;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.query.api.Query;
import com.mawen.nfsdb.journal.query.api.QueryAll;
import com.mawen.nfsdb.journal.query.api.QueryHead;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class QueryImpl<T> implements Query<T> {

	private final Journal<T> journal;
	private final QueryAllImpl<T> allImpl;
	private final QueryHeadImpl<T> headImpl;

	public QueryImpl(Journal<T> journal) {
		this.journal = journal;
		this.allImpl = new QueryAllImpl<>(journal);
		this.headImpl = new QueryHeadImpl<>(journal);
	}

	@Override
	public QueryAll<T> all() {
		return allImpl;
	}

	@Override
	public QueryHead<T> head() {
		return headImpl;
	}

	@Override
	public Journal<T> getJournal() {
		return journal;
	}
}
