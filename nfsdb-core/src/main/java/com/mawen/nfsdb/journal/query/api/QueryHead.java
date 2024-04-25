package com.mawen.nfsdb.journal.query.api;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.map.JournalMap;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public interface QueryHead<T> {

	QueryHeadBuilder<T> withKeys(String... values);

	QueryHeadBuilder<T> withSymValues(String symbol, String... values);

	JournalMap<T> map() throws JournalException;
}
