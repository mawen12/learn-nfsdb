package com.mawen.nfsdb.journal.factory;

import java.io.Closeable;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.JournalKey;
import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public interface JournalReaderFactory extends Closeable {

	void close();

	<T> Journal<T> reader(JournalKey<T> key) throws JournalException;

	<T> Journal<T> reader(Class<T> clazz) throws JournalException;

	<T> Journal<T> reader(Class<T> clazz, String location) throws JournalException;

	JournalConfiguration getConfiguration();
}
