package com.mawen.nfsdb.journal.column;

import java.io.Closeable;

import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public interface MappedFile extends Closeable {

	ByteBufferWrapper getBuffer(long offset, int size);

	void setAppendOffset(long offset);

	long getAppendOffset();

	void compact() throws JournalException;

	void close();
}
