package com.mawen.nfsdb.journal.column;

import java.nio.ByteBuffer;
import java.util.BitSet;

import com.mawen.nfsdb.journal.utils.BitSetAccessor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class NullsColumn extends FixedWidthColumn {
	private final BitSet bitSet;
	private final int wordCount;
	private long cachedRowID;

	public NullsColumn(MappedFile mappedFile, int size, int nullCount) {
		super(mappedFile, size);
		this.bitSet = new BitSet(nullCount);
		this.wordCount = size / 8;
		this.cachedRowID = -1;
	}

	public BitSet getBitSet(long localRowID) {
		if (localRowID != cachedRowID) {
			getBitSet(localRowID, bitSet);
		}
		return bitSet;
	}

	public void putBitSet(BitSet bitSet) {
		ByteBuffer bb = getBuffer();
		long[] words = BitSetAccessor.getWords(bitSet);
		for (int i = 0; i < wordCount; i++) {
			bb.putLong(words[i]);
		}
		cachedRowID = -1;
	}

	private void getBitSet(long localRowID, BitSet bs) {
		ByteBuffer bb = getBuffer(localRowID);
		long[] words = BitSetAccessor.getWords(bs);
		if (words == null || words.length < wordCount) {
			words = new long[wordCount];
			BitSetAccessor.setWords(bs, words);
		}
		BitSetAccessor.setWordsInUse(bs, wordCount);

		for (int i = 0; i < wordCount; i++) {
			words[i] = bb.getLong();
		}
	}

}
