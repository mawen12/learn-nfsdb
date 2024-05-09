package com.mawen.nfsdb.journal.tx;

import java.io.File;
import java.nio.ByteBuffer;

import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.column.MappedFileImpl;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.utils.ByteBuffers;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class TxLog {

	private long address = 0;
	private MappedFileImpl mf;
	private char[] buf;

	public TxLog(File baseLocation, JournalMode mode) throws JournalException {
		this.mf = new MappedFileImpl(new File(baseLocation, "_tx"), JournalConfiguration.PIPE_BIT_HINT, mode);
	}

	public boolean hasNext() {
		return getTxAddress() > address;
	}

	public boolean isEmpty() {
		return mf.getAppendOffset() <= 9;
	}

	public Tx get() {
		long offset = getTxAddress();
		assert offset > 0 : "zero offset";

		Tx tx = new Tx();
		ByteBuffer buffer = mf.getBuffer(offset, 4).getByteBuffer();

		int txSize = buffer.getInt();
		buffer = mf.getBuffer(offset + 4, txSize).getByteBuffer();

		tx.prevTxAddress = buffer.getLong();
		tx.command = buffer.get();
		tx.timestamp = buffer.getLong();
		tx.journalMaxRowID = buffer.getLong();
		tx.lastPartitionTimestamp = buffer.getLong();
		tx.lagSize = buffer.getLong();

		int sz = buffer.get();
		if (sz == 0) {
			tx.lagName = null;
		}
		else {
			// lagName
			sz = buffer.get();
			if (buf == null || buf.length < sz) {
				buf = new char[sz];
			}
			for (int i = 0; i < sz; i++) {
				buf[i] = buffer.getChar();
			}
			tx.lagName = new String(buf, 0, sz);
		}

		// symbolTableSizes
		sz = buffer.getChar();
		tx.symbolTableSizes = new int[sz];
		for (int i = 0; i < sz; i++) {
			tx.symbolTableSizes[i] = buffer.getInt();
		}

		// symbolTableIndexPointers
		sz = buffer.getChar();
		tx.symbolTableIndexPointers = new long[sz];
		for (int i = 0; i < sz; i++) {
			tx.symbolTableIndexPointers[i] = buffer.getLong();
		}

		// indexPointers
		sz = buffer.getChar();
		tx.indexPointers = new long[sz];
		for (int i = 0; i < sz; i++) {
			tx.indexPointers[i] = buffer.getLong();
		}

		// lagIndexPointers
		sz = buffer.getChar();
		tx.lagIndexPointers = new long[sz];
		for (int i = 0; i < sz; i++) {
			tx.lagIndexPointers[i] = buffer.getLong();
		}

		this.address = offset;
		return tx;
	}

	public void create(Tx tx) {

		if (tx.lagName != null && tx.lagName.length() > 64) {
			throw new JournalRuntimeException("Partition name is too long");
		}

		long offset = Math.max(9, mf.getAppendOffset());
		ByteBuffer buffer = mf.getBuffer(offset, tx.size() + 4).getByteBuffer();

		// 4
		buffer.putInt(tx.size());
		// 8
		buffer.putLong(tx.prevTxAddress);
		// 1
		buffer.put(tx.command);
		// 8
		buffer.putLong(System.nanoTime());
		// 8
		buffer.putLong(tx.journalMaxRowID);
		// 8
		buffer.putLong(tx.lastPartitionTimestamp);
		// 8
		buffer.putLong(tx.lagSize);
		// 1
		if (tx.lagName == null) {
			buffer.put((byte) 0);
		}
		else {
			buffer.put((byte) 1);
			// 2
			buffer.put((byte) tx.lagName.length());
			// tx.lagName.len
			for (int i = 0; i < tx.lagName.length(); i++) {
				buffer.putChar(tx.lagName.charAt(i));
			}
		}
		// 2 + 4 * tx.symbolTableSizes.len
		ByteBuffers.putIntW(buffer, tx.symbolTableSizes);
		// 2 + 8 * tx.symbolTableIndexPointers.len
		ByteBuffers.putLongW(buffer, tx.symbolTableIndexPointers);
		// 2 + 8 * tx.indexPointers.len
		ByteBuffers.putLongW(buffer, tx.indexPointers);
		// 2 + 8 * tx.lagIndexPointers.len
		ByteBuffers.putLongW(buffer, tx.lagIndexPointers);

		// write out tx address
		buffer = mf.getBuffer(0, 9).getByteBuffer();
		buffer.mark();
		buffer.put((byte) 0);
		buffer.putLong(offset);
		buffer.reset();
		buffer.put((byte) 1);
		address = offset + tx.size();
		mf.setAppendOffset(address);
	}

	public void close() {
		mf.close();
	}

	private long getTxAddress() {
		if (isEmpty()) {
			return 0;
		}

		ByteBuffer buffer = mf.getBuffer(0, 9).getByteBuffer();
		buffer.mark();
		long limit = 1000;
		while (limit > 0 && buffer.get() == 0) {
			Thread.yield();
			buffer.reset();
			limit--;
		}
		if (limit == 0) {
			throw new JournalRuntimeException("Could not get spin-lock on txLog");
		}
		return buffer.getLong();
	}
}
