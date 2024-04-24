package com.mawen.nfsdb.journal.iterators;

import java.util.List;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.utils.Rows;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class JournalParallelIterator<T> extends AbstractParallelIterator<T> {
	private final Journal<T> journal;
	private final List<JournalIteratorRange> ranges;

	public JournalParallelIterator(Journal journal, List<JournalIteratorRange> ranges, int bufferSize) {
		super(bufferSize);
		this.journal = journal;
		this.ranges = ranges;
	}

	@Override
	public Journal<T> getJournal() {
		return journal;
	}

	@Override
	protected Runnable getRunnable() {
		return () -> new Runnable() {
			private int currentIndex = 0;
			private long currentRowID;
			private long currentUpperBound;
			private int currentPartitionID;
			boolean hasNext = true;

			@Override
			public void run() {
				updateVariables();

				while (!barrier.isAlerted()) {
					try {
						long outSeq = buffer.next();
						Holder<T> holder = buffer.get(outSeq);
						boolean hadNext = hasNext;
						if (hadNext) {
							journal.clearObject(holder.object);
							journal.read(Rows.toRowID(currentPartitionID, currentRowID), holder.object);
							if (currentRowID < currentUpperBound) {
								currentRowID++;
							}
							else {
								currentIndex++;
								updateVariables();
							}
						}
						holder.hasNext = hadNext;
						buffer.publish(outSeq);

						if (!hadNext) {
							break;
						}
					}
					catch (JournalException e) {
						throw new JournalRuntimeException("Error in iterator [%s]", this, e);
					}
				}
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
		};
	}
}
