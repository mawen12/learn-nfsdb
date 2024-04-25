package com.mawen.nfsdb.journal.iterators;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.ResultSet;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class ResultSetParallelIterator<T> extends AbstractParallelIterator<T> {

	private final ResultSet<T> rs;

	public ResultSetParallelIterator(ResultSet<T> rs, int bufferSize) {
		super(bufferSize);
		this.rs = rs;
	}

	@Override
	public Journal<T> getJournal() {
		return rs.getJournal();
	}

	@Override
	protected Runnable getRunnable() {
		return () -> {
			for (int i = 0, size = rs.size(); i < size; i++) {
				try {
					if (barrier.isAlerted()) {
						break;
					}

					long seq = buffer.next();
					Holder<T> holder = buffer.get(seq);
					getJournal().clearObject(holder.object);
					rs.read(i, holder.object);
					buffer.publish(seq);
				}
				catch (JournalException e) {
					throw new JournalRuntimeException("Cannot read ResultSet %s at %d", e, rs, (i - 1));
				}
			}
		};
	}
}
