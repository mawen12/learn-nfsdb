package com.mawen.nfsdb.journal.column;

import java.util.stream.IntStream;

import com.mawen.nfsdb.journal.AbstractTest;
import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unt test for {@link SymbolTable}.
 */
public class SymbolTableUnitTest extends AbstractTest {

	private static final int DATA_SIZE = 500;
	private SymbolTable tab;

	@After
	public void tearDown() {
		if (tab != null) {
			tab.close();
		}
	}

	@Test
	public void testKeyValueMatch() throws JournalException {

		String[] data = createData();
		createTestTable(data);

		// check that keys match values
		try (SymbolTable tab = getReader()) {
			for (int i = 0; i < tab.size(); i++) {
				Assert.assertEquals(data[i], tab.value(i));
			}
		}
	}

	private String[] createData() {
		return IntStream.range(0, DATA_SIZE)
				.mapToObj(i -> "TEST" + i)
				.toArray(String[]::new);
	}

	private void createTestTable(String[] data) throws JournalException {

		if (tab == null) {
			tab = new SymbolTable(DATA_SIZE, 256, factory.getConfiguration().getJournalBase(), "test", JournalMode.APPEND, 0, 0);
		}

		for (String s : data) {
			tab.put(s);
		}

		tab.commit();
	}

	private SymbolTable getReader() throws JournalException {
		return new SymbolTable(DATA_SIZE, 256, factory.getConfiguration().getJournalBase(), "test", JournalMode.APPEND, tab.size(), tab.getIndexTxAddress());
	}

}