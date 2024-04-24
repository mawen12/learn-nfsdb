package com.mawen.nfsdb.journal.iterators;

import java.io.Closeable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public interface ParallelIterator<T> extends JournalIterator<T>, Closeable {

	@Override
	void close();
}
