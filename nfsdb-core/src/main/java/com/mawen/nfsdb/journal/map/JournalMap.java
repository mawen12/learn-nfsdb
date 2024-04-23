package com.mawen.nfsdb.journal.map;

import java.util.Collection;
import java.util.Set;

import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public interface JournalMap<T> {

	JournalMap<T> eager() throws JournalException;

	T get(String key);

	Collection<T> values();

	boolean refresh() throws JournalException;

	Set<String> keys();

	String getColumn();

	int size();
}
