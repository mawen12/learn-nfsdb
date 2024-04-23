package com.mawen.nfsdb.journal.column;

import java.nio.ByteBuffer;

import com.mawen.nfsdb.journal.utils.ByteBuffers;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class ByteBufferWrapper {
	private final long offset;
	private ByteBuffer byteBuffer;

	public ByteBufferWrapper(long offset, ByteBuffer byteBuffer) {
		this.offset = offset;
		this.byteBuffer = byteBuffer;
	}

	public long getOffset() {
		return offset;
	}

	public ByteBuffer getByteBuffer() {
		return byteBuffer;
	}

	public void release() {
		byteBuffer = ByteBuffers.release(byteBuffer);
	}
}
