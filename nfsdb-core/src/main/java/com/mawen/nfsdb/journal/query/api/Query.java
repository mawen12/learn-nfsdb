package com.mawen.nfsdb.journal.query.api;

import com.mawen.nfsdb.journal.Journal;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public interface Query<T> {

	QueryAll<T> all();

	QueryHead<T> head();

	Journal<T> getJournal();
}
