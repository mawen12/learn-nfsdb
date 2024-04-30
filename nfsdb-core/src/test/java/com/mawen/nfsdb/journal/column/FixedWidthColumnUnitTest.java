package com.mawen.nfsdb.journal.column;

import java.io.File;

import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.utils.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class FixedWidthColumnUnitTest {

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
	public void testFixedWidthColumns() throws JournalException {

		// given
		MappedFile mf = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);

		try (FixedWidthColumn pcc = new FixedWidthColumn(mf, 4)) {
			for (int i = 0; i < 10000; i++) {
				pcc.putInt(i);
				pcc.commit();
			}
		}

		// when
		MappedFile mf2 = new MappedFileImpl(dataFile, 22, JournalMode.READ);
		MappedFile mf3 = new MappedFileImpl(dataFile, 22, JournalMode.READ);


		// then
		try (FixedWidthColumn pcc2 = new FixedWidthColumn(mf2, 4)) {
			for (int i = 0; i < 10000; i++) {
				Assert.assertEquals(i, pcc2.getInt(i));
			}
		}

		try (FixedWidthColumn pcc3 = new FixedWidthColumn(mf3, 4)) {
			for (int i = 0; i < 10000; i++) {
				Assert.assertEquals(i, pcc3.getInt(i));
			}
		}
	}

	@Test
	public void testFixedWithFloat() throws JournalException {
		MappedFile data = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
		try (FixedWidthColumn col = new FixedWidthColumn(data, 4)) {
			int max = 150;
			for (int i = 0; i < max; i++) {
				col.putFloat(max - i + 0.33f);
				col.commit();
			}

			for (int i = 0; i < col.size(); i++) {
				Assert.assertEquals(max - i + 0.33f, col.getFloat(i), 0);
			}
		}
	}

	@Test
	public void testFixedWithNull() throws JournalException {
		MappedFile data = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
		try (FixedWidthColumn col = new FixedWidthColumn(data, 4)) {
			int max = 150;
			for (int i = 0; i < max; i++) {
				col.putNull();
				col.commit();
			}

			Assert.assertEquals(max, col.size());
		}
	}

}