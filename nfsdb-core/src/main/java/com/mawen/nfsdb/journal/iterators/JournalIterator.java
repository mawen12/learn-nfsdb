package com.mawen.nfsdb.journal.iterators;

import java.util.Iterator;

import com.mawen.nfsdb.journal.Journal;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public interface JournalIterator<T> extends Iterable<T>, Iterator<T> {

	Journal<T> getJournal();
}
