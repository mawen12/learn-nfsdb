package com.mawen.nfsdb.journal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.management.Query;

import com.mawen.nfsdb.journal.column.SymbolTable;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.logging.Logger;
import com.mawen.nfsdb.journal.query.api.QueryImpl;
import com.mawen.nfsdb.journal.tx.TxLog;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class Journal<T> implements Iterable<T>, Closeable {

	private static final Logger LOGGER = Logger.getLogger(Journal.class);
	public static final long TX_LIMIT_EVAL = -1L;
	protected final List<Partition<T>> partitions = new ArrayList<>();
	protected TxLog txLog;
	private final JournalMetadata<T> metadata;
	private final File location;
	private final Map<String, SymbolTable> symbolTableMap = new HashMap<>();
	private final ArrayList<SymbolTable> symbolTables = new ArrayList<>();
	private final JournalKey<T> key;
	private final Query<T> query = new QueryImpl<>(this);

	@Override
	public void close() throws IOException {

	}

	@Override
	public Iterator<T> iterator() {
		return null;
	}
}
