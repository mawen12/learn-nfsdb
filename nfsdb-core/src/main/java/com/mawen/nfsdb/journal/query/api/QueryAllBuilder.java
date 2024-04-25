package com.mawen.nfsdb.journal.query.api;

import com.mawen.nfsdb.journal.UnorderedResultSet;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public interface QueryAllBuilder<T> {

	QueryAllBuilder<T> limit(Interval interval);

	QueryAllBuilder<T> filter(String symbol, String value);

	void resetFilter();

	UnorderedResultSet<T> asResultSet();
}
