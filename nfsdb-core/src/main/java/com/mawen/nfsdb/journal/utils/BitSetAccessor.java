package com.mawen.nfsdb.journal.utils;

import java.util.BitSet;

import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import sun.misc.Unsafe;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public final class BitSetAccessor {
	private static final long wordsOffset;
	private static final long wordsInUseOffset;

	static {
		try {
			wordsOffset = Unsafe.getUnsafe().objectFieldOffset(BitSet.class.getDeclaredField("words"));
			wordsInUseOffset = Unsafe.getUnsafe().objectFieldOffset(BitSet.class.getDeclaredField("wordsInUse"));
		}
		catch (NoSuchFieldException e) {
			throw new JournalRuntimeException("Incompatible BitSet class", e);
		}
	}

	public static long[] getWords(BitSet instance) {
		return (long[]) Unsafe.getUnsafe().getObject(instance, wordsOffset);
	}

	public static void setWords(BitSet instance, long[] value) {
		Unsafe.getUnsafe().putObject(instance, wordsOffset, value);
	}

	public static void setWordsInUse(BitSet instance, int value) {
		Unsafe.getUnsafe().putInt(instance, wordsInUseOffset, value);
	}
}
