package com.mawen.nfsdb.journal.utils;

import java.nio.ByteBuffer;

import jdk.internal.ref.Cleaner;
import sun.nio.ch.DirectBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public final class ByteBuffers {

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


}
