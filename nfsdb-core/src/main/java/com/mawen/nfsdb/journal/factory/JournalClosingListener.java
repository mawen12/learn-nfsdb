package com.mawen.nfsdb.journal.factory;

import com.mawen.nfsdb.journal.Journal;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public interface JournalClosingListener {

	boolean closing(Journal journal);
}
