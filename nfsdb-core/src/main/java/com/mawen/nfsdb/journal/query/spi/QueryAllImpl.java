package com.mawen.nfsdb.journal.query.spi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.OrderedResultSet;
import com.mawen.nfsdb.journal.OrderedResultSetBuilder;
import com.mawen.nfsdb.journal.Partition;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.iterators.JournalBufferedIterator;
import com.mawen.nfsdb.journal.iterators.JournalIterator;
import com.mawen.nfsdb.journal.iterators.JournalIteratorImpl;
import com.mawen.nfsdb.journal.iterators.JournalIteratorRange;
import com.mawen.nfsdb.journal.iterators.JournalParallelIterator;
import com.mawen.nfsdb.journal.iterators.ParallelIterator;
import com.mawen.nfsdb.journal.query.api.QueryAll;
import com.mawen.nfsdb.journal.query.api.QueryAllBuilder;
import com.mawen.nfsdb.journal.utils.Rows;
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
		return journal.iteratePartitions(new OrderedResultSetBuilder<T>() {
			@Override
			public void read(long lo, long hi) throws JournalException {
				result.ensureCapacity((int) (hi - lo + 1));
				for (long i = lo; i < hi + 1; i++) {
					result.add(Rows.toRowID(partition.getPartitionIndex(), i));
				}
			}
		});

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
	public QueryAllBuilder<T> withKeys(String... values) {
		return withSymValues(journal.getMetadata().getKey(), values);
	}

	@Override
	public QueryAllBuilder<T> withSymValues(String symbol, String... values) {
		QueryAllBuilderImpl<T> result = new QueryAllBuilderImpl<>(journal);
		result.setSymbol(symbol, values);
		return result;
	}

	@Override
	public JournalIterator<T> bufferedIterator() {
		return new JournalBufferedIterator<>(journal, createRanges());
	}

	@Override
	public JournalIterator<T> bufferedIterator(Interval interval) {
		return new JournalBufferedIterator<>(journal, createRanges(interval));
	}

	@Override
	public JournalIterator<T> bufferedIterator(long rowID) {
		return new JournalBufferedIterator<>(journal, createRanges(rowID));
	}

	@Override
	public ParallelIterator<T> parallelIterator() {
		return new JournalParallelIterator<>(journal, createRanges(), 1024);
	}

	@Override
	public ParallelIterator<T> parallelIterator(Interval interval) {
		return new JournalParallelIterator<>(journal, createRanges(interval), 1024);
	}

	@Override
	public ParallelIterator<T> parallelIterator(long rowID) {
		return new JournalParallelIterator<>(journal,createRanges(rowID),1024);
	}

	@Override
	public Iterator<T> iterator() {
		return new JournalIteratorImpl<>(journal, createRanges());
	}

	@Override
	public JournalIterator<T> iterator(Interval interval) {
		return new JournalIteratorImpl<>(journal, createRanges(interval));
	}

	@Override
	public JournalIterator<T> iterator(long rowID) {
		return new JournalIteratorImpl<>(journal, createRanges(rowID));
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

	private List<JournalIteratorRange> createRanges(Interval interval) {
		final List<JournalIteratorRange> ranges = new ArrayList<>();
		try {
			journal.iteratePartitions(new OrderedResultSetBuilder<T>(interval) {
				@Override
				public void read(long lo, long hi) throws JournalException {
					ranges.add(new JournalIteratorRange(partition.getPartitionIndex(), lo, hi));
				}
			});
		}
		catch (JournalException e) {
			throw new JournalRuntimeException(e);
		}
		return ranges;
	}

	private List<JournalIteratorRange> createRanges(long hi) {
		List<JournalIteratorRange> ranges = new ArrayList<>();
		int cilingPartitionID = Rows.toPartitionIndex(hi);
		long ceilingLocalRowID = Rows.toLocalRowID(hi);

		try {
			for (int i = cilingPartitionID; i < journal.getPartitionCount(); i++) {
				long localRowID = 0;
				if (i == cilingPartitionID) {
					localRowID = ceilingLocalRowID;
				}

				Partition<T> p = journal.getPartition(i, true);
				long size = p.size();
				if (size > 0) {
					ranges.add(new JournalIteratorRange(p.getPartitionIndex(), localRowID, size - 1));
				}
			}
			return ranges;
		}
		catch (JournalException e) {
			throw new JournalRuntimeException(e);
		}
	}
}
