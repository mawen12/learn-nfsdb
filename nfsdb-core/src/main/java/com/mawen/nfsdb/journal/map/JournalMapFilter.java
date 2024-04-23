package com.mawen.nfsdb.journal.map;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public interface JournalMapFilter<T> {

	boolean accept(T object);
}
