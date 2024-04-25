package com.mawen.nfsdb.journal.query.api;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public interface QueryHeadBuilder<T> {

	QueryHeadBuilder<T> limit(Interval interval);

	QueryHeadBuilder<T> limit(long minRowID);

	QueryHeadBuilder<T> filter(String symbol, String value);

	QueryHeadBuilder<T> strict(boolean strict);

	void resetFilter();

	UnorderedResultSet<T> asResultSet() throws JournalException;
}
