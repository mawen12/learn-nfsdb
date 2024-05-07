package com.mawen.nfsdb.journal.locks;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import com.mawen.nfsdb.journal.AbstractTest;
import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.JournalWriter;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.factory.JournalFactory;
import com.mawen.nfsdb.journal.test.model.Quote;
import com.mawen.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link Lock}
 */
public class LockUnitTest extends AbstractTest {

	@Test
	public void testLockAcrossClassLoaders() throws JournalException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		URLClassLoader classLoader = new URLClassLoader(((URLClassLoader) this.getClass().getClassLoader()).getURLs(), null);

		factory.getConfiguration().getJournalBase().mkdirs();

		JournalWriter<Quote> rw = factory.writer(Quote.class);
		rw.close();
		rw.delete();

		rw = factory.writer(Quote.class);

		List<Quote> data = new ArrayList<>();
		data.add(new Quote().setSym("S1").setTimestamp(Dates.utc(2023, 5,4,15,0).getMillis()));
		data.add(new Quote().setSym("S2").setTimestamp(Dates.utc(2023, 5,4,16,0).getMillis()));
		rw.appendIrregular(data);
		rw.commit();

		new TestAccessor(factory.getConfiguration().getJournalBase());
		classLoader.loadClass("com.mawen.nfsdb.journal.locks.LockUnitTest$TestAccessor").getConstructor(File.class)
				.newInstance(factory.getConfiguration().getJournalBase());

		rw.close();
		rw.delete();
	}

	public static class TestAccessor {

		public TestAccessor(File journalBase) throws JournalException {
			JournalFactory factory = new JournalFactory(journalBase);
			Journal<Quote> reader = factory.reader(Quote.class);
			Assert.assertEquals(2, reader.size());
			reader.close();
		}
	}
}