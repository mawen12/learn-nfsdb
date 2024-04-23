package com.mawen.nfsdb.journal.column;

import java.io.Closeable;

import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public interface MappedFile extends Closeable {
	ByteBufferWrapper getBuffer(long offset, int size);

	void close();

	long getAppendOffset();

	void setAppendOffset(long offset);

	void compact() throws JournalException;
}
