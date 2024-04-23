package com.mawen.nfsdb.journal.column;

import java.io.Closeable;

import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public abstract class AbstractColumn implements Closeable {
	private final MappedFile mappedFile;
	private long txAppendOffset = -1;

	public abstract void truncate(long size);

	public void preCommit(long appendOffset) {
		this.txAppendOffset = appendOffset;
	}

	public void close() {
		mappedFile.close();
	}

	public long getOffset() {
		return mappedFile.getAppendOffset();
	}

	public abstract long getOffset(long localRowID);

	public void commit() {
		if (txAppendOffset != -1) {
			mappedFile.setAppendOffset(txAppendOffset);
		}
	}

	@Override
	public String toString() {
		return this.getClass().getName() + "[file=" + mappedFile.toString() + ", size=" + size() + "]";
	}

	public abstract long size();

	public ByteBufferWrapper getBuffer(long offset, int size) {
		return mappedFile.getBuffer(offset, size);
	}

	public void compact() throws JournalException {
		mappedFile.compact();
	}

	AbstractColumn(MappedFile storage) {
		this.mappedFile = storage;
	}
}
