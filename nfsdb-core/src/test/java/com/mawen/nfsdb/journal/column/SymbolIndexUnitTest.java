package com.mawen.nfsdb.journal.column;

import java.io.File;

import com.mawen.nfsdb.journal.AbstractTest;
import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.collections.LongArrayList;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link SymbolIndex}
 */
public class SymbolIndexUnitTest extends AbstractTest {

	private static final int totalKeys = 10;
	private static final int totalValues = 100;
	private File indexFile;

	@Before
	public void setUp() throws Exception {
		indexFile = new File(factory.getConfiguration().getJournalBase(), "index-test");
	}

	@Test
	public void testIndexReadWrite() throws JournalException {

		// given
		SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0);
		index.put(0, 0);
		index.put(1, 1);
		index.put(1, 2);
		index.put(0, 3);
		index.put(0, 4);
		index.put(1, 5);
		index.put(1, 6);
		index.put(0, 7);

		// then
		Assert.assertEquals(1, index.getValueQuick(1, 0));
		Assert.assertEquals(2, index.getValueQuick(1, 1));
		Assert.assertEquals(5, index.getValueQuick(1, 2));
		Assert.assertEquals(6, index.getValueQuick(1, 3));
		Assert.assertEquals(0, index.getValueQuick(0, 0));
		Assert.assertEquals(3, index.getValueQuick(0, 1));
		Assert.assertEquals(4, index.getValueQuick(0, 2));
		Assert.assertEquals(7, index.getValueQuick(0, 3));
		Assert.assertEquals(8, index.size());

		index.close();
	}

	@Test(expected = JournalRuntimeException.class)
	public void testValueOutOfBounds() throws JournalException {

		// given
		try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
			index.put(0, 0);
			index.put(1, 1);
			index.put(1, 2);
			index.getValueQuick(0, 1);
		}
	}

	@Test
	public void testKeyOutOfBounds() throws JournalException {
		try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
			index.put(0, 0);
			index.put(1, 1);
			index.put(1, 2);
			Assert.assertEquals(0, index.getValues(2).size());
		}
	}

	@Test
	public void testTruncateMiddle() throws JournalException {

		// given
		try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
			// when
			index.put(0, 0);
			index.put(1, 1);
			index.put(1, 2);
			index.put(0, 3);
			index.put(0, 4);

			index.put(1, 5);
			index.put(1, 6);
			index.put(0, 7);
			index.truncate(5);

			// then
			Assert.assertEquals(3, index.getValues(0).size());
			Assert.assertEquals(2, index.getValues(1).size());

			Assert.assertEquals(1, index.getValueQuick(1, 0));
			Assert.assertEquals(2, index.getValueQuick(1, 1));
			Assert.assertEquals(0, index.getValueQuick(0, 0));
			Assert.assertEquals(3, index.getValueQuick(0, 1));
			Assert.assertEquals(4, index.getValueQuick(0, 2));
		}
	}

	@Test
	public void testTruncateTail() throws JournalException {

		// given
		try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
			// when
			index.put(0, 0);
			index.put(1, 1);
			index.put(1, 2);
			index.put(0, 3);
			index.put(0, 4);
			index.put(1, 5);

			index.put(1, 6);
			index.put(0, 7);
			index.truncate(6);

			// then
			Assert.assertEquals(3, index.getValues(0).size());
			Assert.assertEquals(3, index.getValues(1).size());

			Assert.assertEquals(1, index.getValueQuick(1, 0));
			Assert.assertEquals(2, index.getValueQuick(1, 1));
			Assert.assertEquals(0, index.getValueQuick(0, 0));
			Assert.assertEquals(3, index.getValueQuick(0, 1));
			Assert.assertEquals(4, index.getValueQuick(0, 2));

			Assert.assertEquals(6, index.size());
		}
	}

	@Test
	public void testTruncateBeyondTail() throws JournalException {

		try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
			// when
			index.put(0, 0);
			index.put(1, 1);
			index.put(1, 2);
			index.put(0, 3);
			index.put(0, 4);
			index.put(1, 5);
			index.put(1, 6);
			index.put(0, 7);

			index.truncate(10);

			// then
			Assert.assertEquals(4, index.getValues(0).size());
			Assert.assertEquals(4, index.getValues(1).size());

			Assert.assertEquals(1, index.getValueQuick(1, 0));
			Assert.assertEquals(2, index.getValueQuick(1, 1));
			Assert.assertEquals(5, index.getValueQuick(1, 2));
			Assert.assertEquals(6, index.getValueQuick(1, 3));
			Assert.assertEquals(0, index.getValueQuick(0, 0));
			Assert.assertEquals(3, index.getValueQuick(0, 1));
			Assert.assertEquals(4, index.getValueQuick(0, 2));
			Assert.assertEquals(7, index.getValueQuick(0, 3));

			Assert.assertEquals(8, index.size());
		}
	}

	@Test
	public void testTruncateBeforeStart() throws JournalException {

		// given
		try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
			// when
			index.put(0, 0);
			index.put(1, 1);
			index.put(1, 2);
			index.put(0, 3);
			index.put(0, 4);
			index.put(1, 5);
			index.put(1, 6);
			index.put(0, 7);
			index.truncate(0);

			// then
			Assert.assertFalse(index.contains(0));
			Assert.assertFalse(index.contains(1));
			Assert.assertEquals(0, index.size());
		}
	}

	@Test
	public void testSmallValueArray() throws JournalException {

		// given
		int totalKeys = 2;
		int totalValues = 1;
		try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
			// when
			for (int i = 0; i < totalKeys; i++) {
				index.put(i, i);
			}

			// then
			Assert.assertEquals(totalKeys, index.size());

			index.truncate(0);

			Assert.assertEquals(0, index.size());
		}
	}

	@Test
	public void testAppendNullAfterTruncate() throws JournalException {

		// given
		int totalKeys = 2;
		int totalValues = 1;
		try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
			// when
			for (int i = 0; i < totalKeys; i++) {
				index.put(i, i);
			}

			// then
			Assert.assertEquals(totalKeys, index.size());

			index.truncate(0);

			Assert.assertEquals(0, index.size());
			index.put(-1, 10);
			Assert.assertEquals(11, index.size());
		}
	}

	@Test
	public void testGetValuesMultiBlock() throws JournalException {

		// given
		long[][] expected = {
				{0, 3, 5, 6, 8, 10, 12, 14, 16, 22},
				{1, 2, 3, 4, 6, 8, 9, 11, 16, 21, 33}
		};

		try (SymbolIndex index = new SymbolIndex(indexFile, 10, 60, JournalMode.APPEND, 0)) {
			// when
			putValues(expected, index);
			// then
			assertValues(expected, index);
		}
	}

	@Test
	public void testGetValsPartialBlock() throws JournalException {

		// given
		long[][] expected = {
				{0, 3, 5, 6, 8, 10, 12, 14, 16, 22},
				{1, 2, 3, 4, 6, 8, 9, 11, 16, 21, 33}
		};

		try (SymbolIndex index = new SymbolIndex(indexFile, 10, 200, JournalMode.APPEND, 0)) {
			// when
			putValues(expected, index);
			// then
			assertValues(expected, index);
		}
	}

	@Test
	public void testGetValueQuick() throws JournalException {

		// given
		long[][] expected = {
				{0, 3, 5, 6, 8, 10, 12, 14, 16, 22},
				{1, 2, 3, 4, 6, 8, 9, 11, 16, 21, 33}
		};

		try (SymbolIndex index = new SymbolIndex(indexFile, 10, 60, JournalMode.APPEND, 0)) {
			// when
			putValues(expected, index);

			// then
			for (int i = 0; i < expected.length; i++) {
				Assert.assertEquals(expected[i].length, index.getValueCount(i));
				for (int j = 0; j < expected[i].length; j++) {
					Assert.assertEquals(expected[i][j], index.getValueQuick(i, j));
				}
			}
		}
	}

	@Test
	public void testIndexTx() throws JournalException {

		long[][] expected = {
				{0, 3, 5, 6, 8, 10, 12, 14, 16, 22},
				{1, 2, 3, 4, 6, 8, 9, 11, 16, 21, 33}
		};
		try (SymbolIndex index = new SymbolIndex(indexFile, 10, 60, JournalMode.APPEND, 0);
			 SymbolIndex reader = new SymbolIndex(indexFile, 10, 60, JournalMode.READ, 0)) {

			putValues(expected, index);

			// check if writer looks good
			Assert.assertEquals(34, index.size());
			Assert.assertTrue(index.contains(0));
			Assert.assertTrue(index.contains(1));
			Assert.assertEquals(10, index.getValues(0).size());
			Assert.assertEquals(10, index.getValueCount(0));
			Assert.assertEquals(11, index.getValueCount(1));

			// check if reader #1
			Assert.assertEquals(0, reader.size());
			Assert.assertEquals(0, reader.getValues(0).size());
			Assert.assertFalse(reader.contains(0));
			Assert.assertFalse(reader.contains(1));
			Assert.assertEquals(0, reader.getValueCount(0));
			Assert.assertEquals(0, reader.getValueCount(1));

			// commit writer
			index.commit();

			// check the refreshed reader saw change
			reader.setTxAddress(index.getTxAddress());

			// add some more uncommitted changes to make life more difficult
			index.put(0, 35);
			index.put(1, 46);

			// check if refreshed reader saw only committed changes
			Assert.assertEquals(34, reader.size());
			Assert.assertTrue(reader.contains(0));
			Assert.assertTrue(reader.contains(1));
			Assert.assertEquals(10, reader.getValues(0).size());
			Assert.assertEquals(10, reader.getValueCount(0));
			Assert.assertEquals(11, reader.getValueCount(1));

			// open new reader and check if it can see only committed changes
			try (SymbolIndex reader2 = new SymbolIndex(indexFile, 10, 60, JournalMode.READ, reader.getTxAddress())) {
				Assert.assertEquals(34, reader2.size());
				Assert.assertTrue(reader2.contains(0));
				Assert.assertTrue(reader2.contains(1));
				Assert.assertEquals(10, reader2.getValues(0).size());
				Assert.assertEquals(10, reader2.getValueCount(0));
				Assert.assertEquals(11, reader2.getValueCount(1));
			}

			index.commit();

			try (SymbolIndex reader2 = new SymbolIndex(indexFile, 10, 60, JournalMode.READ, index.getTxAddress())) {
				Assert.assertEquals(47, reader2.size());
				Assert.assertTrue(reader2.contains(0));
				Assert.assertTrue(reader2.contains(1));
				Assert.assertEquals(11, reader2.getValues(0).size());
				Assert.assertEquals(11, reader2.getValueCount(0));
				Assert.assertEquals(12, reader2.getValueCount(1));
			}
		}
	}


	private void putValues(long[][] values, SymbolIndex index) throws JournalException {
		for (int i = 0; i < values.length; i++) {
			for (int j = 0; j < values[i].length; j++) {
				index.put(i, values[i][j]);
			}
		}
	}

	public void assertValues(long[][] values, SymbolIndex index) {
		for (int i = 0; i < values.length; i++) {
			LongArrayList array = index.getValues(i);
			Assert.assertEquals(values[i].length, array.size());
			for (int j = 0; j < values[i].length; j++) {
				Assert.assertEquals(values[i][j], array.get(j));
			}
		}
	}
}