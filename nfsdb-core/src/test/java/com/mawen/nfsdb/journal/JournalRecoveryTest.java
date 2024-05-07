package com.mawen.nfsdb.journal;

import java.util.Random;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.test.model.Quote;
import com.mawen.nfsdb.journal.utils.Dates;
import com.mawen.nfsdb.journal.utils.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Recovery test for {@link Journal}
 */
public class JournalRecoveryTest extends AbstractTest {

	@Test
	public void testRecovery() throws Exception {

		factory.getConfiguration().getJournalBase().mkdirs();

		try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
			w.setAutoCommit(false);
			assertFalse(w.isAutoCommit());
			TestUtils.generateQuoteData(w, 10000, Dates.interval("2023-01-01T00:00:00.000Z", "2023-03-30T12:55:00.000Z"));
			assertEquals("2023-03-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
			TestUtils.generateQuoteData(w, 10000, Dates.interval("2023-03-01T00:00:00.000Z", "2023-05-30T12:55:00.000Z"), false);
			assertEquals("2023-05-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
		}

		try (Journal<Quote> w = factory.reader(Quote.class)) {
			assertEquals("2023-03-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
			assertEquals(9999, w.size());
		}

		try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
			w.setAutoCommit(false);
			assertEquals("2023-03-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
			assertEquals(9999, w.size());
		}
	}

	@Test
	public void testLagRecovery() throws JournalException {

		factory.getConfiguration().getJournalBase().mkdirs();

		JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
		TestUtils.generateQuoteData(origin, 100000, Dates.interval("2023-01-01T00:00:00.000Z", "2023-05-30T12:55:00.000Z"));

		try (Journal<Quote> r = factory.reader(Quote.class, "origin")) {
			assertEquals(100000, r.size());
		}

		Random r = new Random(System.currentTimeMillis());
		try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
			w.setAutoCommit(false);
			w.append(origin.query().all().asResultSet().subset(0, 15000));
			w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(15000, 17000).shuffle(r).read()));
			w.commit();
			w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(17000, 27000).shuffle(r).read()));
			w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(27000, 37000).shuffle(r).read()));
			assertEquals("2023-02-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
			assertEquals(37000, w.size());
		}

		try (Journal<Quote> w = factory.reader(Quote.class)) {
			assertEquals("2023-01-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
			assertEquals(17000, w.size());
		}

		try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
			assertEquals("2023-01-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
			assertEquals(17000, w.size());
		}
	}
}