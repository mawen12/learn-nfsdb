package com.mawen.nfsdb.journal.column;

import java.nio.ByteBuffer;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.logging.Logger;
import com.mawen.nfsdb.journal.utils.ByteBuffers;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class VarcharColumn extends AbstractColumn{
	private static final Logger LOGGER = Logger.getLogger(VarcharColumn.class);

	private final FixedWidthColumn indexColumn;
	private final int maxLen;
	private char[] buffer = new char[32];

	public VarcharColumn(MappedFile dataFile, MappedFile indexFile, int maxLen) {
		super(dataFile);
		this.indexColumn = new FixedWidthColumn(indexFile, JournalConfiguration.VARCHAR_INDEX_COLUMN_WIDTH);
		this.maxLen = maxLen;
	}

	@Override
	public void commit() {
		super.commit();
		indexColumn.commit();
	}

	@Override
	public void close() {
		indexColumn.close();
		super.close();
	}

	@Override
	public void truncate(long size) {
		if (size < 0) {
			size = 0;
		}

		if (size < size()) {
			preCommit(getOffset(size));
		}
		indexColumn.truncate(size);
	}

	@Override
	public long size() {
		return indexColumn.size();
	}

	@Override
	public long getOffset(long localRowID) {
		return indexColumn.getLong(localRowID);
	}

	public void putString(String value) {
		if (value == null) {
			putNull();
		}
		else if (maxLen <= JournalMetadata.BYTE_LIMIT) {
			putStringB(value);
		}
		else if (maxLen <= JournalMetadata.TWO_BYTE_LIMIT) {
			putStringW(value);
		}
		else {
			putStringDW(value);
		}
	}

	public String getString(long localRowID) {
		try {
			if (maxLen <= JournalMetadata.BYTE_LIMIT) {
				return getStringB(localRowID);
			}
			else if (maxLen <= JournalMetadata.TWO_BYTE_LIMIT) {
				return getStringW(localRowID);
			}
			else {
				return getStringDW(localRowID);
			}
		}
		catch (RuntimeException e) {
			LOGGER.error(this + " Could not read string [localRowID=" + localRowID + "]");
			throw e;
		}
	}

	@Override
	public void compact() throws JournalException {
		super.compact();
		this.indexColumn.compact();
	}

	public FixedWidthColumn getIndexColumn() {
		return indexColumn;
	}

	public void putNull() {
		ByteBufferWrapper buf = getBufferInternal(1);
		ByteBuffer bb = buf.getByteBuffer();
		long rowOffset = buf.getOffset() + bb.position();
		commitAppend(rowOffset, 0);
	}

	/////////////////////////////////////////////////////////////////

	ByteBufferWrapper getBufferInternal(int recordLength) {
		return getBuffer(getOffset(), recordLength);
	}

	void commitAppend(long offset, int size) {
		preCommit(offset + size);
		indexColumn.putLong(offset);
	}

	/////////////////////////////////////////////////////////////////

	private void putStringB(String value) {
		int len = value.length();
		ByteBufferWrapper buf = getBufferInternal(len * 2 + JournalConfiguration.VARCHAR_SHORT_HEADER_LENGTH);
		ByteBuffer bb = buf.getByteBuffer();
		long rowOffset = buf.getOffset() + bb.position();
		ByteBuffers.putStringB(bb, value);
		commitAppend(rowOffset, JournalConfiguration.VARCHAR_SHORT_HEADER_LENGTH + 2 * len);
	}

	private String getStringB(long localRowID) {
		// read delegate buffer which lets us read "null" flag and string length.
		ByteBuffer buf = getBufferInternal(localRowID, JournalConfiguration.VARCHAR_SHORT_HEADER_LENGTH).getByteBuffer();
		int len = buf.get() & 0xff;
		// check if buffer can have actual string (char=2*byte)
		if (buf.remaining() < len * 2) {
			buf = getBufferInternal(localRowID, len * 2 + JournalConfiguration.VARCHAR_SHORT_HEADER_LENGTH).getByteBuffer();
			buf.position(buf.position() + JournalConfiguration.VARCHAR_SHORT_HEADER_LENGTH);
		}

		return asString(buf, len);
	}

	private void putStringW(String value) {
		int len = value.length() * 2 + JournalConfiguration.VARCHAR_MEDIUM_HEADER_LENGTH;
		ByteBufferWrapper buf = getBufferInternal(len);
		ByteBuffer bb = buf.getByteBuffer();
		long rowOffset = buf.getOffset() + bb.position();
		ByteBuffers.putStringW(bb, value);
		commitAppend(rowOffset, len);
	}

	private String getStringW(long localRowID) {
		// read delegate buffer which lets us read "null" flag and string length.
		ByteBuffer bb = getBufferInternal(localRowID, JournalConfiguration.VARCHAR_MEDIUM_HEADER_LENGTH).getByteBuffer();
		int len = bb.getChar();
		// check if buffer can actual string (char=2*byte)
		if (bb.remaining() < len * 2) {
			bb = getBufferInternal(localRowID, len * 2 + JournalConfiguration.VARCHAR_MEDIUM_HEADER_LENGTH).getByteBuffer();
			bb.position(bb.position() + JournalConfiguration.VARCHAR_MEDIUM_HEADER_LENGTH);
		}

		return asString(bb, len);
	}

	private void putStringDW(String value) {
		int len = value.length() * 2 + JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH;
		ByteBufferWrapper buf = getBufferInternal(len);
		ByteBuffer bb = buf.getByteBuffer();
		long rowOffset = buf.getOffset() + bb.position();
		ByteBuffers.putStringDW(bb, value);
		commitAppend(rowOffset, len);
	}

	private String getStringDW(long localRowID) {
		// read delegate buffer which lets us read "null" flag and string length.
		ByteBuffer bb = getBufferInternal(localRowID, JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH).getByteBuffer();
		int len = bb.getInt();
		// check if buffer can have actual string (char=2*byte)
		if (bb.remaining() < len * 2) {
			bb = getBufferInternal(localRowID, len * 2 + JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH).getByteBuffer();
			bb.position(bb.position() + JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH);
		}

		return asString(bb, len);
	}

	private ByteBufferWrapper getBufferInternal(long localRowID, int recordLength) {
		long max = indexColumn.size();

		if (localRowID > max) {
			throw new JournalRuntimeException("localRowID is out of bounds. %d > %d", localRowID, max);
		}

		if (localRowID == max) {
			return getBuffer(getOffset(), recordLength);
		}
		else {
			return getBuffer(getOffset(localRowID), recordLength);
		}
	}

	private String asString(ByteBuffer bb, int len) {
		if (buffer.length < len) {
			buffer = new char[len];
		}

		for (int i = 0; i < len; i++) {
			buffer[i] = bb.getChar();
		}
		return new String(buffer, 0, len);
	}
}
