package com.mawen.nfsdb.journal.iterators;

import java.util.Iterator;
import java.util.List;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalImmutableIteratorException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.utils.Rows;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class JournalIteratorImpl<T> implements JournalIterator<T> {

	private final List<JournalIteratorRange> ranges;
	private final Journal<T> journal;
	private boolean hasNext = true;
	private int currentIndex = 0;
	private long currentRowID;
	private long currentUpperBound;
	private int currentPartitionID;

	public JournalIteratorImpl(Journal<T> journal, List<JournalIteratorRange> ranges) {
		this.ranges = ranges;
		this.journal = journal;
		updateVariables();
		hasNext = hasNext && currentRowID <= currentUpperBound;
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public T next() {
		try {
			T result = journal.read(Rows.toRowID(currentPartitionID, currentRowID));
			if (currentRowID < currentUpperBound) {
				currentRowID++;
			}
			else {
				currentIndex++;
				updateVariables();
			}

			return result;
		}
		catch (JournalException e) {
			throw new JournalRuntimeException("Error in iterator [%s]", e, this);
		}
	}

	@Override
	public void remove() {
		throw new JournalImmutableIteratorException();
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}

	@Override
	public Journal<T> getJournal() {
		return journal;
	}

	@Override
	public String toString() {
		return "JournalIteratorImpl{" +
		       "currentRowID=" + currentRowID +
		       ", currentUpperBound=" + currentUpperBound +
		       ", currentPartitionID=" + currentPartitionID +
		       ", currentIndex=" + currentIndex +
		       ", journal=" + journal +
		       '}';
	}

	private void updateVariables() {
		if (currentIndex < ranges.size()) {
			JournalIteratorRange w = ranges.get(currentIndex);
			currentRowID = w.lowerRowIDBound;
			currentUpperBound = w.upperRowIDBound;
			currentPartitionID = w.partitionID;
		}
		else {
			hasNext = false;
		}
	}
}
