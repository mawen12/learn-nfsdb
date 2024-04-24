package com.mawen.nfsdb.journal.query.spi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.Partition;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.iterators.JournalIterator;
import com.mawen.nfsdb.journal.iterators.JournalIteratorRange;
import com.mawen.nfsdb.journal.iterators.ParallelIterator;
import com.mawen.nfsdb.journal.query.api.QueryAll;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class QueryAllImpl<T> implements QueryAll<T> {

	private final Journal<T> journal;

	public QueryAllImpl(Journal<T> journal) {
		this.journal = journal;
	}

	@Override
	public OrderedResultSet<T> asResultSet() throws JournalException {

	}

	@Override
	public long size() {
		try {
			return journal.size();
		}
		catch (Exception e) {
			throw new JournalRuntimeException(e);
		}
	}

	@Override
	public QueryAllBuilder<T> withKeys(String... keys) {
		return null;
	}

	@Override
	public QueryAllBuilder<T> withSymValues(String symbol, String... value) {
		return null;
	}

	@Override
	public JournalIterator<T> bufferedIterator() {
		return null;
	}

	@Override
	public ParallelIterator<T> parallelIterator() {
		return null;
	}

	@Override
	public JournalIterator<T> iterator(Interval interval) {
		return null;
	}

	@Override
	public JournalIterator<T> bufferedIterator(Interval interval) {
		return null;
	}

	@Override
	public ParallelIterator<T> parallelIterator(Interval interval) {
		return null;
	}

	@Override
	public JournalIterator<T> iterator(long rowID) {
		return null;
	}

	@Override
	public JournalIterator<T> bufferedIterator(long rowID) {
		return null;
	}

	@Override
	public ParallelIterator<T> parallelIterator(long rowID) {
		return null;
	}

	@Override
	public Iterator<T> iterator() {
		return null;
	}

	private List<JournalIteratorRange> createRanges() {
		final int partitionCount = journal.getPartitionCount();
		List<JournalIteratorRange> ranges = new ArrayList<>(partitionCount);
		try {
			for (int i = 0; i < partitionCount; i++) {
				Partition<T> p = journal.getPartition(i, true);
				long size = p.size();
				if (size > 0) {
					ranges.add(new JournalIteratorRange(p.getPartitionIndex(), 0, size - 1));
				}
			}
		}
		catch (JournalException e) {
			throw new JournalRuntimeException(e);
		}
		return ranges;
	}
}
