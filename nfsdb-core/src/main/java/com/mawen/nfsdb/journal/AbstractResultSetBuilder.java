package com.mawen.nfsdb.journal;

import com.mawen.nfsdb.journal.collections.LongArrayList;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public abstract class AbstractResultSetBuilder<T, X> {

	protected final LongArrayList result = new LongArrayList();
	protected Partition<T> partition;
	protected Journal<T> journal;
	protected Interval interval;

	public void setJournal(Journal<T> journal) {
		this.journal = journal;
	}

	public boolean next(Partition<T> partition, boolean desc) throws JournalException {

		if (interval != null && partition.getInterval() != null &&
				(partition.getInterval().getStartMillis() > interval.getEndMillis() ||
						partition.getInterval().getEndMillis() < interval.getStartMillis())) {
			return (partition.getInterval().getEndMillis() < interval.getStartMillis() && !desc) ||
					(partition.getInterval().getStartMillis() > interval.getEndMillis() && desc);
		}

		switch (accept(partition)) {
			case SKIP:
				return false;
			case BREAK:
				return true;
		}

		long size = partition.open().size();

		if (size > 0) {

			long lo = 0;
			long hi = size - 1;

			if (interval != null && partition.getInterval() != null) {
				if (partition.getInterval().getStartMillis() > interval.getStartMillis()) {
					long _lo = partition.indexOf(interval.getStartMillis(), BinarySearch.SearchType.GREATER_OR_EQUAL);

					// there are no data with timestamp later than start date of interval, skip partition
					if (_lo == -2) {
						return false;
					}

					lo = _lo;
				}

				if (partition.getInterval().getEndMillis() > interval.getEndMillis()) {
					long _hi = partition.indexOf(interval.getEndMillis(), BinarySearch.SearchType.LESS_OR_EQUAL);

					// there are no data with timestamp earlier then end date of interval, skip partition
					if (_hi == -1) {
						return false;
					}

					hi = _hi;
				}
			}

			if (lo <= hi) {
				read(lo, hi);
			}
		}
		return false;
	}

	public Accept accept(Partition<T> partition) throws JournalException {
		this.partition = partition;
		return Accept.CONTINUE;
	}

	public abstract void read(long lo, long hi) throws JournalException;

	public abstract X getResult();

	protected AbstractResultSetBuilder(Interval interval) {
		this.interval = interval;
	}

	protected AbstractResultSetBuilder(){}

	public enum Accept {
		CONTINUE, SKIP, BREAK
	}
}
