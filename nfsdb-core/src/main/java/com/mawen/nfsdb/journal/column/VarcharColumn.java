package com.mawen.nfsdb.journal.column;

import java.nio.ByteBuffer;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.logging.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class VarcharColumn extends AbstractColumn{
	private static final Logger LOGGER = Logger.getLogger(VarcharColumn.class);

	private final FixedWidthColumn indexColumn;
	private final int maxLen;
	private char buffer[] = new char[32];

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

	public String getString(long localRowID) {
		try {
			if (maxLen < JournalMetadata.BYTE_LIMIT) {
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

	private String getStringB(long localRowID) {
		// read delegate buffer which lets us read "null" flag and string length.
		getBufferInternal(localRowID, )
	}
}
