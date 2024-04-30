package com.mawen.nfsdb.journal.column;

import java.io.File;
import java.util.Arrays;

import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.utils.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class VarcharColumnUnitTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();
	private File dataFile;
	private File indexFile;

	@Before
	public void setUp() throws Exception {
		dataFile = new File(temporaryFolder.getRoot(), "col.d");
		indexFile = new File(temporaryFolder.getRoot(), "col.i");
	}

	@After
	public void tearDown() throws Exception {
		Files.deleteOrException(dataFile);
		Files.deleteOrException(indexFile);
	}

	@Test
	public void testSingleVarchar() throws JournalException {
		// given
		MappedFile df1 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
		MappedFile idx1 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

		try (VarcharColumn varchar1 = new VarcharColumn(df1, idx1, JournalMetadata.BYTE_LIMIT)) {
			varchar1.putString("s");
			varchar1.commit();
		}

		// when
		MappedFile df2 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
		MappedFile idx2 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

		// then
		try (VarcharColumn varchar2 = new VarcharColumn(df2, idx2, JournalMetadata.BYTE_LIMIT)) {
			Assert.assertEquals(1, varchar2.size());
			String s = varchar2.getString(0);
			Assert.assertEquals("s", s);
		}
	}

	@Test
	public void testVarcharColumn() throws JournalException {

		// given
		final int recordCount = 10000;

		MappedFile df1 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
		MappedFile idx1 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

		try (VarcharColumn varchar1 = new VarcharColumn(df1, idx1, JournalMetadata.BYTE_LIMIT)) {
			for (int i = 0; i < recordCount; i++) {
				varchar1.putString("s" + i);
				varchar1.commit();
			}
		}

		// when
		MappedFile df2 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
		MappedFile idx2 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

		// then
		try (VarcharColumn varchar2 = new VarcharColumn(df2, idx2, JournalMetadata.BYTE_LIMIT)) {
			Assert.assertEquals(recordCount, varchar2.size());
			for (int i = 0; i < recordCount; i++) {
				String s = varchar2.getString(i);
				Assert.assertEquals("s" + i, s);
			}
		}
	}

	@Test
	public void testVarcharNulls() throws JournalException {

		// given
		MappedFile df1 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
		MappedFile idx1 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

		try (VarcharColumn varchar1 = new VarcharColumn(df1, idx1, JournalMetadata.BYTE_LIMIT)) {
			varchar1.putString("string1"); // 0
			varchar1.commit();
			varchar1.putString("string2"); // 1
			varchar1.commit();
			varchar1.putNull(); // 2
			varchar1.commit();
			varchar1.putString("string3"); // 3
			varchar1.commit();
			varchar1.putNull();// 4
			varchar1.commit();
			varchar1.putString("string4"); // 5
			varchar1.commit();
		}

		// when
		MappedFile df2 = new MappedFileImpl(dataFile, 22, JournalMode.READ);
		MappedFile idx2 = new MappedFileImpl(indexFile, 22, JournalMode.READ);

		// then
		try (VarcharColumn varchar2 = new VarcharColumn(df2, idx2, JournalMetadata.BYTE_LIMIT)) {
			Assert.assertEquals("string1", varchar2.getString(0));
			Assert.assertEquals("string2", varchar2.getString(1));
			Assert.assertEquals("string3", varchar2.getString(3));
			Assert.assertEquals("string4", varchar2.getString(5));
		}
	}

	@Test
	public void testTruncate() throws JournalException {

		// given
		MappedFile df1 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
		MappedFile idx1 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);
		try (VarcharColumn varchar = new VarcharColumn(df1, idx1, JournalMetadata.BYTE_LIMIT)) {
			putCommit(varchar, "string1");
			putCommit(varchar, "string2");
			putCommit(varchar);
			putCommit(varchar, "string3");
			putCommit(varchar);
			putCommit(varchar, "string4");

			Assert.assertEquals(6, varchar.size());
			varchar.truncate(4);
			varchar.commit();

			Assert.assertEquals(4, varchar.size());
			Assert.assertEquals("string1", varchar.getString(0));
			Assert.assertEquals("string2", varchar.getString(1));
//			Assert.assertNull(varchar.getString(2));
			Assert.assertEquals("string3", varchar.getString(3));
		}

		// when
		MappedFile df2 = new MappedFileImpl(dataFile, 22, JournalMode.READ);
		MappedFile idx2 = new MappedFileImpl(indexFile, 22, JournalMode.READ);

		// then
		try (VarcharColumn varchar = new VarcharColumn(df2, idx2, JournalMetadata.BYTE_LIMIT)) {
			Assert.assertEquals("string1", varchar.getString(0));
			Assert.assertEquals("string2", varchar.getString(1));
//			Assert.assertNull(varchar.getString(2));
			Assert.assertEquals("string3", varchar.getString(3));
		}
	}

	private void putCommit(VarcharColumn varcharColumn, String... content) {
		if (content.length == 0) {
			varcharColumn.putNull();
		}
		else {
			varcharColumn.putString(content[0]);
		}
		varcharColumn.commit();
	}
}