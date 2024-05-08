package com.mawen.nfsdb.journal;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.test.model.Quote;
import com.mawen.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Empty Test for {@link Journal}
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/8
 */
public class EmptyJournalTest extends AbstractTest {

	@Before
	public void before() throws JournalException {
		factory.getConfiguration().getJournalBase().mkdirs();
	}

	@Test
	public void testEmptyJournalIterator() throws JournalException {
		testJournalIterator(factory.writer(com.mawen.nfsdb.journal.test.model.Quote.class));
	}

	@Test
	public void testJournalWithEmptyPartition() throws JournalException {
		JournalWriter<Quote> w = factory.writer(Quote.class);
		w.getAppendPartition(Dates.toMillis("2012-02-10T00:00:00.000Z"));
		w.getAppendPartition(Dates.toMillis("2012-03-10T00:00:00.000Z"));
		testJournalIterator(w);
	}

	private void testJournalIterator(Journal journal) {
		Assert.assertFalse(journal.iterator().hasNext());
	}

}
