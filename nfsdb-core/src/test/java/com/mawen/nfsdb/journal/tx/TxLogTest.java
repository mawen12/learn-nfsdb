package com.mawen.nfsdb.journal.tx;

import java.io.File;
import java.io.IOException;

import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalNoSuchFileException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class TxLogTest {

	@Rule
	public final TemporaryFolder temp = new TemporaryFolder();

	@After
	public void after() throws IOException {
		temp.delete();
	}

	@Test
	public void testTxLog() throws IOException, JournalException {
		File dir = temp.newFolder();
		TxLog txLog = new TxLog(dir, JournalMode.APPEND);

		assertFalse(txLog.hasNext());

		txLog.close();
	}

	@Test
	public void testTxLog2() throws Exception {
		File dir = temp.newFolder();

		assertThrows(JournalNoSuchFileException.class, () -> {
			TxLog txLog = new TxLog(dir, JournalMode.READ);
		});
	}

	@Test
	public void testTxLog3() throws Exception{
		File dir = temp.newFolder();
		TxLog txLog = new TxLog(dir, JournalMode.APPEND);

		Tx tx = new Tx();
		tx.prevTxAddress = 99999;
		tx.command = 0;
		tx.journalMaxRowID = 10;
		tx.lagSize = 12;
		tx.lagName = "abcrrrrrrrrrrrrrrrrrrrrrrrrrrr";
		tx.timestamp = 1000001L;
		tx.lastPartitionTimestamp = 200002L;
		tx.symbolTableSizes = new int[] {10, 12};
		tx.symbolTableIndexPointers = new long[] {2, 15, 18};
		tx.indexPointers = new long[] {36, 48};
		tx.lagIndexPointers = new long[] {55, 67};

		txLog.create(tx);

		assertFalse(txLog.hasNext());
	}

	@Test
	public void testTxLog4() throws Exception{
		File dir = temp.newFolder();
		TxLog txLog = new TxLog(dir, JournalMode.APPEND);

		Tx tx = new Tx();
		tx.prevTxAddress = 99999;
		tx.command = 0;
		tx.journalMaxRowID = 10;
		tx.lagSize = 12;
		tx.lagName = "abcrrrrrrrrrrrrrrrrrrrrrrrrrrr";
		tx.timestamp = 1000001L;
		tx.lastPartitionTimestamp = 200002L;
		tx.symbolTableSizes = new int[] {10, 12};
		tx.symbolTableIndexPointers = new long[] {2, 15, 18};
		tx.indexPointers = new long[] {36, 48};
		tx.lagIndexPointers = new long[] {55, 67};

		txLog.create(tx);

		TxLog r = new TxLog(dir, JournalMode.READ);
		assertTrue(r.hasNext());
		Tx tx1 = r.get();
		assertEquals(0, tx1.command);
		assertEquals(10, tx.journalMaxRowID);
		assertEquals(12, tx.lagSize);
		assertEquals("abcrrrrrrrrrrrrrrrrrrrrrrrrrrr", tx.lagName);
		assertEquals(1000001L, tx.timestamp);
		assertEquals(200002L, tx.lastPartitionTimestamp);

		assertArrayEquals(new int[] {10, 12}, tx.symbolTableSizes);
		assertArrayEquals(new long[] {2, 15, 18}, tx.symbolTableIndexPointers);
		assertArrayEquals(new long[] {36, 48}, tx.indexPointers);
		assertArrayEquals(new long[] {55, 67}, tx.lagIndexPointers);

		assertFalse(r.hasNext());

		txLog.close();
		r.close();
	}

}