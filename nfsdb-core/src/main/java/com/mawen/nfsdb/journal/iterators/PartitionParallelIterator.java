package com.mawen.nfsdb.journal.iterators;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.Partition;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class PartitionParallelIterator<T> extends AbstractParallelIterator<T> {
	private final Partition<T> partition;
	private final long lo;
	private final long hi;

	public PartitionParallelIterator(Partition<T> partition, long lo, long hi, int bufferSize) {
		super(bufferSize);
		this.partition = partition;
		this.lo = lo;
		this.hi = hi;
	}

	@Override
	public Journal<T> getJournal() {
		return partition.getJournal();
	}

	@Override
	protected Runnable getRunnable() {
		return () -> {

			for (long i = lo; i < hi; i++) {
				try {
					partition.open();
					if (barrier.isAlerted()) {
						break;
					}

					long seq = buffer.next();
					Holder<T> holder = buffer.get(seq);
					partition.getJournal().clearObject(holder.object);
					partition.read(i, holder.object);
					buffer.publish(seq);
				}
				catch (JournalException e) {
					throw new JournalRuntimeException("Cannot read partition %s at %d", partition, i - 1, e);
				}
			}

			long seq = buffer.next();
			buffer.get(seq).hasNext = false;
			buffer.publish(seq);
		};
	}
}
