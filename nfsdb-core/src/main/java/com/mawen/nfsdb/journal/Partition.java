package com.mawen.nfsdb.journal;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import com.mawen.nfsdb.journal.logging.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class Partition<T> implements Iterable<T>, Closeable {
	private static final Logger LOGGER = Logger.getLogger(Partition.class);
	private final Journal<T> journal;
	private final ArrayList<SymbolIndexProxy<T>> indexProxies = new ArrayList<>();
	private final ArrayList<SymbolIndexProxy<T>> columnIndexProxies = new ArrayList<>();

	@Override
	public void close() throws IOException {

	}

	@Override
	public Iterator<T> iterator() {
		return null;
	}
}
