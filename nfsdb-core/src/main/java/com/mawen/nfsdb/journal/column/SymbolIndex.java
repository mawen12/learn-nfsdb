package com.mawen.nfsdb.journal.column;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.collections.LongArrayList;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.utils.ByteBuffers;
import com.mawen.nfsdb.journal.utils.Files;

/**
 * Storage for row count and offset. block structure in kData is [int, long] for header and [long, long] for [offset, count]
 *
 * <pre>{@code
 * 		struct kdata {
 * 		 	int rowBlockSize
 * 		 	long firstEntryOffset
 * 		 	struct kdataEntry {
 * 		 	  	long offsetOfTail
 * 		 	  	long rowCount
 * 		 	}
 * 		}
 * }</pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class SymbolIndex implements Closeable {

	static final byte[] ZERO_BYTE_ARRAY = new byte[4096];

	static {
		Arrays.fill(ZERO_BYTE_ARRAY, (byte) 0);
	}

	private static final int ENTRY_SIZE = 16;

	private final int keyCountHint;
	private MappedFileImpl kData;
	// storage for rows
	// block structure is [rowid1, rowid2, ..., rowidn, prevBlockOffset]
	private MappedFileImpl rData;
	private int rowBlockSize;
	private int rowBlockLen;
	private long firstEntryOffset;
	private long keyBlockAddressOffset;
	private long keyBlockSizeOffset;
	private long keyBlockSize;
	private long maxValue;
	private boolean inTransaction = false;


	public SymbolIndex(File baseName, int keyCountHint, int recordCountHint, JournalMode mode, long txAddress) throws JournalException {
		this.keyCountHint = Math.max(keyCountHint, 1);
		this.rowBlockLen = Math.max(recordCountHint / this.keyCountHint / 2, 1);
		this.kData = new MappedFileImpl(new File(baseName.getParentFile(), baseName.getName() + ".k"), ByteBuffers.getBitHint(8 + 8, this.keyCountHint), mode);
		this.keyBlockAddressOffset = 8;

		this.keyBlockSizeOffset = 16;
		this.keyBlockSize = 0;
		this.maxValue = 0;

		if (kData.getAppendOffset() > 0) {
			this.rowBlockLen = (int) getLong(kData, 0);
			this.keyBlockSizeOffset = txAddress == 0 ? getLong(kData, keyBlockAddressOffset) : txAddress;
			this.keyBlockSize = getLong(kData, keyBlockSizeOffset);
			this.maxValue = getLong(kData, keyBlockSizeOffset + 8);
		}
		else if (mode == JournalMode.APPEND) {
			putLong(kData, 0, this.rowBlockLen);
			putLong(kData, keyBlockAddressOffset, keyBlockSizeOffset);
			putLong(kData, keyBlockSizeOffset, keyBlockSize);
			putLong(kData, keyBlockSizeOffset + 8, maxValue);
			kData.setAppendOffset(8 + 8 + 8 + 8);
		}

		this.firstEntryOffset = keyBlockSizeOffset + 16;
		this.rowBlockSize = rowBlockLen * 8 + 8;
		this.rData = new MappedFileImpl(new File(baseName.getParentFile(), baseName.getName() + ".r"), ByteBuffers.getBitHint(rowBlockSize, keyCountHint * 2), mode);
	}


	public static void delete(File base) {
		Files.delete(new File(base.getParentFile(), base.getName() + ".k"));
		Files.delete(new File(base.getParentFile(), base.getName() + ".r"));
	}

	/**
	 * Adds value to index. Values will be stored in same order as they were added.
	 *
	 * @param key   value of key
	 * @param value value
	 */
	public void put(int key, long value) throws JournalException {

		tx();

		long keyOffset = getKeyOffset(key);
		long rowBlockOffset;
		long rowCount;

		if (keyOffset >= firstEntryOffset + keyBlockSize) {
			long oldSize = keyBlockSize;
			keyBlockSize = keyOffset + ENTRY_SIZE - firstEntryOffset;
			// if keys are added in random order there will be gaps in key block with possibly random values
			// to mitigate that as soon as we see an attempt to extend key block past ENTRY_SIZE we need to
			// fill created gap with zeroes.
			if (keyBlockSize - oldSize > ENTRY_SIZE) {
				ByteBuffer buf = kData.getBuffer(firstEntryOffset + oldSize, (int) (keyBlockSize - oldSize - ENTRY_SIZE)).getByteBuffer();
				while (buf.hasRemaining()) {
					buf.put(ZERO_BYTE_ARRAY, 0, Math.min(ZERO_BYTE_ARRAY.length, buf.remaining()));
				}
			}
		}

		ByteBuffer buf = kData.getBuffer(keyOffset, ENTRY_SIZE).getByteBuffer();
		buf.mark();
		rowBlockOffset = buf.getLong();
		rowCount = buf.getLong();
		buf.reset();

		int cellIndex = (int) (rowCount % rowBlockLen);
		if (rowBlockOffset == 0 || cellIndex == 0) {
			long prevBlockOffset = rowBlockOffset;
			rowBlockOffset = rData.getAppendOffset() + rowBlockSize;
			rData.setAppendOffset(rowBlockOffset);
			putLong(rData, rowBlockOffset - 8, prevBlockOffset);
		}
		putLong(rData, rowBlockOffset - rowBlockSize + 8 * cellIndex, value);
		buf.putLong(rowBlockOffset);
		buf.putLong(rowCount + 1);

		if (maxValue <= value) {
			maxValue = value + 1;
		}
	}

	public void setTxAddress(long txAddress) {
		if (txAddress == 0) {
			refresh();
		}
		else {
			this.keyBlockSizeOffset = txAddress;
			this.keyBlockSize = getLong(kData, keyBlockSizeOffset);
			this.maxValue = getLong(kData, keyBlockSizeOffset + 8);
			this.firstEntryOffset = keyBlockSizeOffset + 16;
		}
	}

	public long getTxAddress() {
		return keyBlockSizeOffset;
	}

	public void refresh() {
		commit();
		this.keyBlockSizeOffset = getLong(kData, keyBlockAddressOffset);
		this.keyBlockSize = getLong(kData, keyBlockSizeOffset);
		this.maxValue = getLong(kData, keyBlockSizeOffset + 8);
		this.firstEntryOffset = keyBlockSizeOffset + 16;
	}

	public void commit() {
		if (inTransaction) {
			putLong(kData, keyBlockSizeOffset, keyBlockSize);
			putLong(kData, keyBlockSizeOffset + 8, maxValue);
			kData.setAppendOffset(firstEntryOffset + keyBlockSize);
			putLong(kData, keyBlockAddressOffset, keyBlockSizeOffset);
			inTransaction = false;
		}
	}

	/**
	 * Searches for indexed value of a key. This method will lookup newest values much faster than oldest.
	 * If either key doesn't exist in index or value index is out of bounds an exception will be thrown.
	 *
	 * @param key value of key
	 * @param i index of key value to get.
	 * @return long value for given index
	 */
	public long getValueQuick(int key, int i) {

		ByteBuffer buf = keyBufferOrError(key);

		long rowBlockOffset = buf.getLong();
		long rowCount = buf.getLong();

		if (i >= rowCount) {
			throw new JournalRuntimeException("Index out of bounds: %d, max: %d", i, rowCount - 1);
		}

		int rowBlockCount = (int) (rowCount / rowBlockLen + 1);
		if (rowCount % rowBlockLen == 0) {
			rowBlockCount--;
		}

		int targetBlock = i / rowBlockLen;
		int cellIndex = i % rowBlockLen;

		while (targetBlock < --rowBlockCount) {
			rowBlockOffset = getLong(rData, rowBlockOffset - 8);
			if (rowBlockOffset == 0) {
				throw new JournalRuntimeException("Count doesn't match number of row blocks. Corrupt index? : %s", this);
			}
		}

		return getLong(rData, rowBlockOffset - rowBlockSize + 8 * cellIndex);
	}

	/**
	 * Checks if key exists in index. Use case for this method is in combination with {@lastValue} method
	 *
	 * @param key value of key
	 * @return true if key has any values associated with it, false otherwise.
	 */
	public boolean contains(int key) {
		return getValueCount(key) > 0;
	}

	/**
	 * Counts values for a key. Use case for this method is best illustrated by this code sample:
	 *
	 * <pre>{@code
	 * 	int c = index.getValueCount(key);
	 * 	for (int i = c-1; i >= 0; i--) {
	 * 	    long value = index.getValueQuick(key, i);
	 * 	}
	 * }</pre>
	 *
	 * <p>Note that the loop above is deliberately in reverse order, as {@link #getValueQuick(int, int)} performance
	 * diminishes the father away from last the current index is.
	 *
	 * <p>If it is your intention to iterate through all index values consider {@link #getValues}, as it offers better
	 * for this use case.
	 *
	 * @param key value of key
	 * @return numbers of values associated with key. 0 if either key doesn't exist or it doesn't have values.
	 */
	public int getValueCount(int key) {
		long keyOffset = getKeyOffset(key);
		if (keyOffset >= firstEntryOffset + keyBlockSize) {
			return 0;
		}
		else {
			ByteBuffer buf = kData.getBuffer(keyOffset + 8, 8).getByteBuffer();
			return (int) buf.getLong();
		}
	}

	/**
	 * Gets last value for key. If key doesn't exist in index an exception will be thrown.
	 * This method has to be used in combination with #contains. E.g. check if index contains key and
	 * if it does - get last value. Thrown exception is intended to point out breaking of the convention.
	 *
	 * @param key value of key
	 * @return value
	 */
	public long lastValue(int key) {
		ByteBuffer buf = keyBufferOrError(key);
		long rowBlockOffset = buf.getLong();
		long rowCount = buf.getLong();
		int cellIndex = (int) ((rowCount - 1) % rowBlockLen);
		return getLong(rData, rowBlockOffset - rowBlockSize + 8 * cellIndex);
	}

	/**
	 * List of values for key. Values are in order they were added.
	 * This method is best for use cases where you are most likely to process all records from index.
	 * In cases where you need last few records from index {@link #getValueCount(int)} and {@link #getValueQuick(int, int)}
	 * are faster and more memory efficient.
	 *
	 * @param key key value
	 * @return List of values or exception if key doesn't exist.
	 */
	public LongArrayList getValues(int key) {
		LongArrayList result = new LongArrayList();
		getValues(key, result);
		return result;
	}

	/**
	 * This method does the same thing as {@link #getValues(int)}. In addition to it lets calling party to reuse their
	 *
	 * @param key key value
	 * @param values the array to copy values to. The contents of this array will be overwritten with new values beginning from 0 index.
	 */
	public void getValues(int key, LongArrayList values) {

		values.resetQuick();

		if (key < 0) {
			return;
		}

		long keyOffset = getKeyOffset(key);
		if (keyOffset >= firstEntryOffset + keyBlockSize) {
			return;
		}
		ByteBuffer buf = kData.getBuffer(keyOffset, ENTRY_SIZE).getByteBuffer();
		long rowBlockOffset = buf.getLong();
		long rowCount = buf.getLong();

		values.setCapacity((int) rowCount);
		values.setPos((int) rowCount);

		int rowBlockCount = (int) (rowCount / rowBlockLen) + 1;
		int len = (int) (rowCount % rowBlockLen);
		if (len == 0) {
			rowBlockCount--;
			len = rowBlockLen;
		}

		for (int i = rowBlockCount - 1; i >= 0; i--) {
			ByteBuffer b = rData.getBuffer(rowBlockOffset - rowBlockSize, rowBlockSize).getByteBuffer();
			int z = i * rowBlockLen;
			for (int k = 0; k < len; k++) {
				values.setQuick(z + k, b.getLong());
			}
			b.position(b.position() + (rowBlockLen - len) * 8);
			rowBlockOffset = b.getLong();
			len = rowBlockLen;
		}
	}

	/**
	 * Size of index is in fact maximum of all row IDs. This is useful to keep it in same units of measure as
	 * size of columns.
	 *
	 * @return max of all row IDs in index.
	 */
	public long size() {
		return maxValue;
	}

	/**
	 * Closes underlying files.
	 */
	@Override
	public void close() {
		rData.close();
		kData.close();
	}

	/**
	 * Remove empty space at end of index files. This is useful if your chosen file copy routine does not support
	 * sparse files, e.g. where size of file content significantly smaller than file size in directory catalogue.
	 */
	public void compact() throws JournalException {
		kData.compact();
		rData.compact();
	}

	public void truncate(long size) {
		long offset = firstEntryOffset;
		long sz = 0;
		while (offset < firstEntryOffset + keyBlockSize) {
			ByteBuffer bb = kData.getBuffer(offset, ENTRY_SIZE).getByteBuffer();
			bb.mark();
			long rowBlockOffset = bb.getLong();
			long rowCount = bb.getLong();
			int len = (int) (rowCount % rowBlockLen);

			if (len == 0) {
				len = rowBlockLen;
			}
			while (rowBlockOffset > 0) {
				ByteBuffer buf = rData.getBuffer(rowBlockOffset - rowBlockSize, rowBlockSize).getByteBuffer();
				buf.mark();
				int pos = 0;
				long max = -1;
				while (pos < len) {
					long v = buf.getLong();
					if (v >= size) {
						break;
					}
					pos++;
					max = v;
				}

				if (max >= sz) {
					sz = max + 1;
				}

				if (pos == 0) {
					// discard whole block
					buf.reset();
					buf.position(buf.position() + rowBlockSize - 8);
					rowBlockOffset = buf.getLong();
					rowCount -= len;
					len = rowBlockLen;
				}
				else {
					rowCount -= len - pos;
					break;
				}
			}
			bb.reset();
			bb.putLong(rowBlockOffset);
			bb.putLong(rowCount);
			offset += ENTRY_SIZE;
		}

		maxValue = sz;
		commit();
	}

	/////////////////////////////////////////////////////////////////

	private void tx() throws JournalException {
		if (!inTransaction) {
			this.keyBlockSizeOffset = kData.getAppendOffset();
			this.firstEntryOffset = keyBlockSizeOffset + 16;

			try (MappedFileImpl kReader = new MappedFileImpl(new File(kData.getFullFileName()), ByteBuffers.getBitHint(8 + 8, keyCountHint), JournalMode.READ)) {
				long srcOffset = getLong(kReader, keyBlockAddressOffset);
				long dstOffset = this.keyBlockSizeOffset;
				long size = this.keyBlockSize + 8 + 8;
				ByteBuffer src = null;
				ByteBuffer dst = null;

				while (size > 0) {
					if (src == null || !src.hasRemaining()) {
						src = kReader.getBuffer(srcOffset, 1).getByteBuffer();
					}
					if (dst == null || !dst.hasRemaining()) {
						dst = kData.getBuffer(dstOffset, 1).getByteBuffer();
					}

					int limit = src.limit();
					int len;
					try {
						if (src.remaining() > size) {
							src.limit(src.position() + (int) (size));
						}
						len = ByteBuffers.copy(src, dst);
						size -= len;
					} finally {
						src.limit(limit);
					}
					srcOffset += len;
					dstOffset += len;
				}

				keyBlockSize = dstOffset - firstEntryOffset;
			}

			inTransaction = true;
		}
	}

	private void putLong(MappedFileImpl storage, long offset, long value) {
		storage.getBuffer(offset, 8).getByteBuffer().putLong(value);
	}

	private long getLong(MappedFileImpl storage, long offset) {
		return storage.getBuffer(offset, 8).getByteBuffer().getLong();
	}

	private ByteBuffer keyBufferOrError(int key) {
		long keyOffset = getKeyOffset(key);
		if (keyOffset >= firstEntryOffset + keyBlockSize) {
			throw new JournalRuntimeException("Key doesn't exists: %d", key);
		}
		return kData.getBuffer(keyOffset, ENTRY_SIZE).getByteBuffer();
	}

	private long getKeyOffset(int key) {
		return firstEntryOffset + (key + 1) * ENTRY_SIZE;
	}
}
