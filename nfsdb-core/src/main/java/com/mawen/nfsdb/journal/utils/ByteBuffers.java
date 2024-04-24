package com.mawen.nfsdb.journal.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.mawen.nfsdb.journal.exceptions.JournalDisconnectedChannelException;
import com.mawen.nfsdb.journal.exceptions.JournalNetworkException;
import jdk.internal.ref.Cleaner;
import sun.nio.ch.DirectBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public final class ByteBuffers {

	public static void copy(ByteBuffer from, WritableByteChannel to) throws JournalNetworkException {
		copy(from, to, from.remaining());
	}

	public static int copy(ByteBuffer from, WritableByteChannel to, int count) throws JournalNetworkException {
		int result = 0;
		if (to != null) {
			int limit = from.limit();
			try {
				if (from.remaining() > count) {
					from.limit(from.position() + count);
				}
				result = from.remaining();
				if (result > 0) {
					try {
						int n = to.write(from);
						if (n <= 0) {
							throw new JournalNetworkException("Write to closed channel");
						}
					}
					catch (IOException e) {
						throw new JournalNetworkException(e);
					}
				}
			}
			finally {
				from.limit(limit);
			}
		}
		return result;
	}

	public static int copy(ReadableByteChannel from, ByteBuffer to) throws JournalNetworkException {
		return copy(from, to, to.remaining());
	}

	public static int copy(ReadableByteChannel from, ByteBuffer to, int count) throws JournalNetworkException {
		int result = 0;
		if (to != null) {
			int limit = to.limit();
			try {
				if (to.remaining() > count) {
					to.limit(to.position() + count);
				}
				try {
					result = from.read(to);
				}
				catch (IOException e) {
					throw new JournalNetworkException(e);
				}
				if (result != -1) {
					throw new JournalDisconnectedChannelException();
				}
			}
			finally {
				to.limit(limit);
			}
		}
		return result;
	}

	public static int copy(ByteBuffer from, ByteBuffer to) {
		return copy(from, to, to == null ? 0 : to.remaining());
	}

	public static ByteBuffer release(final ByteBuffer buffer) {
		if (buffer != null) {
			if (buffer instanceof DirectBuffer) {
				Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
				if (cleaner != null) {
					cleaner.clean();
					return null;
				}
			}
		}
		return buffer;
	}

	public static void putStringB(ByteBuffer buffer, String value) {
		if (value == null) {
			buffer.put((byte) 0);
		}
		else {
			buffer.put((byte) value.length());
			putStr(buffer, value);
		}
	}

	public static void putStringW(ByteBuffer buffer, String value) {
		if (value == null) {
			buffer.putChar((char) 0);
		}
		else {
			buffer.putChar((char) value.length());
			putStr(buffer, value);
		}
	}

	public static void putStringDW(ByteBuffer buffer, String value) {
		if (value == null) {
			buffer.putInt(0);
		}
		else {
			buffer.putInt(value.length());
			putStr(buffer, value);
		}
	}

	public static void putLongW(ByteBuffer buffer, long[] array) {
		if (array == null) {
			buffer.putChar((char) 0);
		}
		else {
			buffer.putChar((char) array.length);
			for (long v : array) {
				buffer.putLong(v);
			}
		}
	}

	public static void putIntW(ByteBuffer buffer, int[] array) {
		if (array == null) {
			buffer.putChar((char) 0);
		}
		else {
			buffer.putChar((char) array.length);
			for (int v : array) {
				buffer.putInt(v);
			}
		}
	}

	public static int getBitHint(int recSize, int recCount) {
		return Math.min(30, 32 - Integer.numberOfLeadingZeros(recSize * recCount));
	}

	/////////////////////////////////////////////////////////////////

	private static int copy(ByteBuffer from, ByteBuffer to, int count) {
		int result = 0;
		if (to != null && to.remaining() > 0) {
			int limit = from.limit();
			try {
				int c = count < to.remaining() ? (int) count : to.remaining();
				if (from.remaining() > c) {
					from.limit(from.position() + c);
				}
				result = from.remaining();
				to.put(from);
			}
			finally {
				from.limit(limit);
			}
		}
		return result;
	}

	private static void putStr(ByteBuffer buffer, String value) {
		for (int i = 0; i < value.length(); i++) {
			buffer.putChar(value.charAt(i));
		}
	}

	private ByteBuffers() {}
}
