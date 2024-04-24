package com.mawen.nfsdb.journal.utils;

import java.lang.reflect.Field;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public final class Unsafe {

	private static final sun.misc.Unsafe UNSAFE;

	public static sun.misc.Unsafe getUnsafe() {
		return UNSAFE;
	}

	static {
		try {
			Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			UNSAFE = (sun.misc.Unsafe)theUnsafe.get(null);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Unsafe() {}

}
