package com.mawen.nfsdb.journal;

import java.util.ArrayList;
import java.util.List;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.test.model.Quote;
import com.mawen.nfsdb.journal.utils.Dates;
import com.mawen.nfsdb.journal.utils.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Refresh test for {@link Journal}
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/7
 */
public class JournalRefreshTest extends AbstractTest {

	private JournalWriter<Quote> rw;

	@Before
	public void before() throws JournalException {
		factory.getConfiguration().getJournalBase().mkdirs();
		rw = factory.writer(Quote.class);
	}

	@Test
	public void testRefreshScenarios() throws JournalException {

		// initial data
		rw.append(new Quote().setSym("IMO-1").setTimestamp(Dates.utc(2013, 1, 10, 10, 0).getMillis()));
		rw.append(new Quote().setSym("IMO-2").setTimestamp(Dates.utc(2013, 1, 10, 14, 0).getMillis()));
		rw.commit();

		Journal<Quote> r = factory.reader(Quote.class);
		assertEquals(2, r.size());

		// append data to same partition
		rw.append(new Quote().setSym("IMO-1").setTimestamp(Dates.utc(2013, 1, 10, 15, 0).getMillis()));
		rw.append(new Quote().setSym("IMO-2").setTimestamp(Dates.utc(2013, 1, 10, 16, 0).getMillis()));
		rw.commit();

		// check that size didn't change before we call refresh
		assertEquals(2, r.size());

		// check that we see two more rows after refresh
		r.refresh();
		assertEquals(4, r.size());

		// append data to new partition
		rw.append(new Quote().setSym("IMO-3").setTimestamp(Dates.utc(2013, 2,10,15,0).getMillis()));
		rw.append(new Quote().setSym("IMO-4").setTimestamp(Dates.utc(2013, 2, 10, 16, 0).getMillis()));

		// check that size didn't change before we call refresh
		assertEquals(4, r.size());

		// check that we don't see rows even if wee refresh
		r.refresh();
		assertEquals(4, r.size());

		rw.commit();
		// check that we see two more rows after refresh
		r.refresh();
		assertEquals(6, r.size());

		List<Quote> data = new ArrayList<>();
		data.add(new Quote().setSym("IMO-5").setTimestamp(Dates.utc(2013, 3, 10, 15, 0).getMillis()));
		data.add(new Quote().setSym("IMO-6").setTimestamp(Dates.utc(2013, 3, 10, 16, 0).getMillis()));
		rw.appendIrregular(data);

		rw.commit();

		// check that size didn't change before we call refresh
		assertEquals(6, r.size());

		// check that we see two more rows after refresh
		r.refresh();
		assertEquals(8, r.size());
	}

	@Test
	public void testTruncateRefresh() throws JournalException {

		TestUtils.generateQuoteData(rw, 1000, Dates.toMillis("2013-09-04T10:00:00.000Z"));
		rw.commit();

		Journal<Quote> r = factory.reader(Quote.class);

		assertEquals(10, r.getSymbolTable("sym").size());
		r.getSymbolTable("sym").preLoad();

		rw.truncate();

		assertTrue(r.refresh());
		assertEquals(0, r.size());
		assertEquals(0, r.getSymbolTable("sym").size());
	}

	@Test
	public void testPartitionRescan() throws JournalException {

		Journal<Quote> reader = factory.reader(Quote.class);
		assertEquals(0, reader.size());

		TestUtils.generateQuoteData(rw, 1001);
		reader.refresh();
		assertEquals(1001, reader.size());

		TestUtils.generateQuoteData(rw, 302, Dates.toMillis("2024-02-10T10:00:00.000Z"));
		reader.refresh();
		assertEquals(1001, reader.size());

		rw.commit();
		reader.refresh();
		assertEquals(1303, reader.size());
	}

	@Test
	public void testIllegalArgExceptionInStorage() throws JournalException {

		rw.append(new Quote().setMode("A").setSym("B").setEx("E1").setAsk(10).setAskSize(1000).setBid(9).setBidSize(900).setTimestamp(System.currentTimeMillis()));
		rw.compact();
		rw.commit();

		Journal<Quote> reader = factory.reader(Quote.class);
		reader.query().all().asResultSet().read();
		rw.close();

		JournalWriter<Quote> writer = factory.writer(Quote.class);
		writer.append(new Quote().setMode("A").setSym("B").setEx("E1").setAsk(10).setAskSize(1000).setBid(9).setBidSize(900).setTimestamp(System.currentTimeMillis()));

		Quote expected = new Quote().setMode("A").setSym("B22").setEx("E1").setAsk(10).setAskSize(1000).setBid(9).setBidSize(900).setTimestamp(System.currentTimeMillis());
		writer.append(expected);
		writer.commit();

		reader.refresh();
		ResultSet<Quote> rs = reader.query().all().asResultSet();
		// at this point we used to get all IllegalArgumentException because we
		// were reaching outside of buffer of compacted column
		assertEquals(3, rs.size());
		Quote q = rs.read(rs.size() - 1);
		assertEquals(expected, q);
	}

	@Test
	public void testLagDetach() throws JournalException {
		JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
		Journal<Quote> reader = factory.reader(Quote.class);

		TestUtils.generateQuoteData(origin, 500, Dates.toMillis("2014-02-10T02:00:00.000Z"));
		TestUtils.generateQuoteData(origin, 500, Dates.toMillis("2014-02-10T10:00:00.000Z"));

		rw.append(origin.query().all().asResultSet().subset(0, 500));
		rw.commit();
		reader.refresh();
		assertEquals(rw.size(), reader.size());

		rw.append(origin.query().all().asResultSet().subset(500, 600));
		rw.commit();
		reader.refresh();
		assertEquals(rw.size(), reader.size());

		rw.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(500, 600).read()));
		rw.commit();
		reader.refresh();
		assertEquals(rw.size(), reader.size());

		rw.removeIrregularPartition();
		rw.commit();
		reader.refresh();
		assertEquals(rw.size(), reader.size());
	}

	@Test
	public void testReadConsistency() throws JournalException {

		Quote q1 = new Quote().setSym("ABC").setEx("LN");
		Quote q2 = new Quote().setSym("EFG").setEx("SK");

		rw.append(q1);
		rw.close();

		rw = factory.writer(Quote.class);

		JournalWriter<Quote> r = factory.writer(Quote.class);

		for (Quote v : r) {
			assertEquals(q1, v);
		}

		rw.append(q2);

		for (Quote v : r) {
			assertEquals(q1, v);
		}
	}
}
