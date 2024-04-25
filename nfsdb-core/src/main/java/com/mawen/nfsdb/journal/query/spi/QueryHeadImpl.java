package com.mawen.nfsdb.journal.query.spi;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.map.JournalHashMap;
import com.mawen.nfsdb.journal.map.JournalMap;
import com.mawen.nfsdb.journal.query.api.QueryHead;
import com.mawen.nfsdb.journal.query.api.QueryHeadBuilder;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class QueryHeadImpl<T> implements QueryHead<T> {

	private final Journal<T> journal;

	public QueryHeadImpl(Journal<T> journal) {
		this.journal = journal;
	}

	@Override
	public QueryHeadBuilder<T> withKeys(String... values) {
		return withSymValues(journal.getMetadata().getKey(), values);
	}

	@Override
	public QueryHeadBuilder<T> withSymValues(String symbol, String... values) {
		QueryHeadBuilderImpl<T> impl = new QueryHeadBuilderImpl<>(journal);
		impl.setSymbol(symbol, values);
		return impl;
	}

	@Override
	public JournalMap<T> map() throws JournalException {
		return new JournalHashMap<>(this.journal).eager();
	}
}
