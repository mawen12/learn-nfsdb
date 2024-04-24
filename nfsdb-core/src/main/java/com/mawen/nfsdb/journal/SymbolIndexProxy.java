package com.mawen.nfsdb.journal;

import java.io.Closeable;
import java.io.IOException;

import com.mawen.nfsdb.journal.column.SymbolIndex;
import com.mawen.nfsdb.journal.logging.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class SymbolIndexProxy<T> implements Closeable {

	private static final Logger LOGGER = Logger.getLogger(SymbolIndexProxy.class);

	private final Partition<T> partition;
	private final int columnIndex;
	private SymbolIndex index;
	private volatile long lastAccessed;
	private long txAddress;

	@Override
	public void close() throws IOException {
		if (index != null) {
			LOGGER.trace("Closing " + this);
			index.close();
			index = null;
		}
		lastAccessed = 0L;
	}

	public long getLastAccessed() {
		return lastAccessed;
	}

	public void setTxAddress(long txAddress) {
		this.txAddress = txAddress;
		if (index != null) {
			index.setTxAddress(txAddress);
		}
	}

	@Override
	public String toString() {
		return "SymbolIndexProxy{" +
				"index=" + index +
				", lastAccessed=" + lastAccessed +
				'}';
	}

	SymbolIndexProxy(Partition<T> partition, int columnIndex, long txAddress) {
		this.partition = partition;
		this.columnIndex = columnIndex;
		this.txAddress = txAddress;
	}
}
