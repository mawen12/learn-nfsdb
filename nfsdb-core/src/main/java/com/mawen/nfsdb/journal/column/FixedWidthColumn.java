package com.mawen.nfsdb.journal.column;

import java.nio.ByteBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class FixedWidthColumn extends AbstractColumn {
	private final int width;


	public FixedWidthColumn(MappedFile mappedFile, int width) {
		super(mappedFile);
		this.width = width;
	}


	public ByteBuffer getBuffer(long localRowID) {
		return getBuffer(getOffset(localRowID), width).getByteBuffer();
	}

	public void putBool(boolean value) {
		getBuffer().put((byte) (value ? 1 : 0));
	}

	public void putByte(byte value) {
		getBuffer().put(value);
	}

	public void putShort(short value) {
		getBuffer().putShort(value);
	}

	public void putInt(int value) {
		getBuffer().putInt(value);
	}

	public void putLong(long value) {
		getBuffer().putLong(value);
	}

	public void putFloat(float value) {
		getBuffer().putFloat(value);
	}

	public void putDouble(double value) {
		getBuffer().putDouble(value);
	}

	public void putNull() {
		getBuffer();
		preCommit(getOffset() + width);
	}

	public boolean getBool(long localRowID) {
		return getBuffer(localRowID).get() == 1;
	}

	public byte getByte(long localRowID) {
		return getBuffer(localRowID).get();
	}

	public short getShort(long localRowID) {
		return getBuffer(localRowID).getShort();
	}

	public int getInt(long localRowID) {
		return getBuffer(localRowID).getInt();
	}

	public long getLong(long localRowID) {
		return getBuffer(localRowID).getLong();
	}

	public float getFloat(long localRowID) {
		return getBuffer(localRowID).getFloat();
	}

	public double getDouble(long localRowID) {
		return getBuffer(localRowID).getDouble();
	}

	@Override
	public void truncate(long size) {
		if (size < 0) {
			size = 0;
		}
		preCommit(size * width);
	}

	@Override
	public long getOffset(long localRowID) {
		return localRowID * width;
	}

	@Override
	public long size() {
		return getOffset() / width;
	}

	ByteBuffer getBuffer() {
		long appendOffset = getOffset();
		ByteBufferWrapper buf = getBuffer(appendOffset, width);
		preCommit(appendOffset + width);
		return buf.getByteBuffer();
	}
}
