package com.mawen.nfsdb.journal;

import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public abstract class UnorderedResultSetBuilder<T> extends AbstractResultSetBuilder<T, UnorderedResultSet<T>> {

	@Override
	public UnorderedResultSet<T> getResult() {
		return new UnorderedResultSet<>(journal, result);
	}

	protected UnorderedResultSetBuilder(Interval interval) {
		super(interval);
	}
}
