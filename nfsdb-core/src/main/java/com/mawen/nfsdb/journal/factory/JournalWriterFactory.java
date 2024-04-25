package com.mawen.nfsdb.journal.factory;

import com.mawen.nfsdb.journal.JournalWriter;
import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public interface JournalWriterFactory {

	<T> JournalWriter<T> writer(Class<T> clazz) throws JournalException;

	<T> JournalWriter<T> writer(Class<T> clazz, String location) throws JournalException;

	<T> JournalWriter<T> writer(Class<T> clazz, String location, int recordHint) throws JournalException;

	JournalConfiguration getConfiguration();
}
