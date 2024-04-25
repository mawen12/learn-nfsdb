package com.mawen.nfsdb.journal;

import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public abstract class OrderedResultSetBuilder<T> extends AbstractResultSetBuilder<T, OrderedResultSet<T>> {

	@Override
	public OrderedResultSet<T> getResult() {
		return new OrderedResultSet<>(journal, result);
	}

	protected OrderedResultSetBuilder(Interval interval) {
		super(interval);
	}

	protected OrderedResultSetBuilder() {
		super();
	}

}
