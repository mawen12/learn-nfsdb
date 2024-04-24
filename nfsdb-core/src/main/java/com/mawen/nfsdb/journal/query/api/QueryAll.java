package com.mawen.nfsdb.journal.query.api;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.iterators.JournalIterator;
import com.mawen.nfsdb.journal.iterators.ParallelIterator;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public interface QueryAll<T> extends Iterable<T> {

	OrderedResultSet<T> asResultSet() throws JournalException;

	long size();

	JournalIterator<T> bufferedIterator();

	ParallelIterator<T> parallelIterator();

	JournalIterator<T> iterator(Interval interval);

	QueryAllBuilder<T> withKeys(String... keys);

	QueryAllBuilder<T> withSymValues(String symbol, String... value);

	JournalIterator<T> bufferedIterator(Interval interval);

	ParallelIterator<T> parallelIterator(Interval interval);

	JournalIterator<T> iterator(long rowID);

	JournalIterator<T> bufferedIterator(long rowID);

	ParallelIterator<T> parallelIterator(long rowID);
}
