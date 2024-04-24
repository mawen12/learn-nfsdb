package com.mawen.nfsdb.journal.factory;

import com.mawen.nfsdb.journal.exceptions.JournalConfigurationException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public interface NullsAdaptorFactory {

	<T> NullsAdaptor<T> getInstance(Class<T> type) throws JournalConfigurationException;
}
