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
public class JournalBufferedIterator<T> implements JournalIterator<T> {
	boolean hasNext = true;
	private final List<JournalIteratorRange> ranges;
	private final Journal<T> journal;
	private final T obj;
	private int currentIndex = 0;
	private long currentRowID;
	private long currentUpperBound;
	private int currentPartitionID;

	public JournalBufferedIterator(Journal<T> journal, List<JournalIteratorRange> ranges) {
		this.ranges = ranges;
		this.journal = journal;
		this.obj = journal.newObject();
		updateVariables();
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public T next() {
		try {
			journal.clearObject(obj);
			journal.read(Rows.toRowID(currentPartitionID, currentRowID), obj);
			if (currentRowID < currentUpperBound) {
				currentRowID++;
			}
			else {
				currentIndex++;
				updateVariables();
			}
			return obj;
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
		return "JournalBufferedIterator{" +
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
