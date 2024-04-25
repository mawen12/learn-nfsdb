package com.mawen.nfsdb.journal.iterators;

import java.util.Iterator;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.Partition;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalImmutableIteratorException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class PartitionBufferedIterator<T> implements JournalIterator<T> {

	private final long hi;
	private final T obj;
	private final Partition<T> partition;
	private long cursor;

	public PartitionBufferedIterator(Partition<T> partition, long lo, long hi) {
		this.partition = partition;
		this.hi = hi;
		this.cursor = lo;
		this.obj = partition.getJournal().newObject();
	}

	@Override
	public boolean hasNext() {
		return cursor <= hi;
	}

	@Override
	public T next() {
		try {
			if (!partition.isOpen()) {
				partition.open();
			}

			partition.getJournal().clearObject(obj);
			partition.read(cursor++, obj);
			return obj;
		}
		catch (JournalException e) {
			throw new JournalRuntimeException("Cannot read partition %s at %d", partition, cursor - 1, e);
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
		return partition.getJournal();
	}
}
