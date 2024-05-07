package com.mawen.nfsdb.journal;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.test.model.Quote;
import com.mawen.nfsdb.journal.utils.Dates;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for {@link Partition}
 */
public class PartitionUnitTest extends AbstractTest {

	@Test
	public void testIndexOf() throws JournalException {

		factory.getConfiguration().getJournalBase().mkdirs();
		JournalWriter<Quote> journal = factory.writer(Quote.class);

		long ts1 = Dates.toMillis("2012-06-05T00:00:00.000");
		long ts2 = Dates.toMillis("2012-07-03T00:00:00.000");
		long ts3 = Dates.toMillis("2012-06-04T00:00:00.000");
		long ts4 = Dates.toMillis("2012-06-06T00:00:00.000");

		Quote q9 = new Quote().setSym("S5").setTimestamp(ts3);
		Quote q10 = new Quote().setSym("S5").setTimestamp(ts4);

		Quote q1 = new Quote().setSym("S1").setTimestamp(ts1);
		Quote q2 = new Quote().setSym("S2").setTimestamp(ts1);
		Quote q3 = new Quote().setSym("S3").setTimestamp(ts1);
		Quote q4 = new Quote().setSym("S4").setTimestamp(ts1);

		Quote q5 = new Quote().setSym("S1").setTimestamp(ts2);
		Quote q6 = new Quote().setSym("S2").setTimestamp(ts2);
		Quote q7 = new Quote().setSym("S3").setTimestamp(ts2);
		Quote q8 = new Quote().setSym("S4").setTimestamp(ts2);

		journal.append(q9);
		journal.append(q1);
		journal.append(q2);
		journal.append(q3);
		journal.append(q4);
		journal.append(q10);

		journal.append(q5);
		journal.append(q6);
		journal.append(q7);
		journal.append(q8);

		assertEquals(2, journal.getPartitionCount());

		long tsA = Dates.toMillis("2012-06-15T00:00:00.000");

		Partition<Quote> partition1 = journal.getPartitionForTimestamp(tsA).open();
		assertNotNull("getPartition(timestamp) failed", partition1);

		assertEquals(-2, partition1.indexOf(tsA, BinarySearch.SearchType.GREATER_OR_EQUAL));
		assertEquals(-1, partition1.indexOf(Dates.toMillis("2012-06-03T00:00:00.000"), BinarySearch.SearchType.LESS_OR_EQUAL));
		assertEquals(0, partition1.indexOf(Dates.toMillis("2012-06-03T00:00:00.000"), BinarySearch.SearchType.GREATER_OR_EQUAL));

		assertEquals(4, partition1.indexOf(ts1, BinarySearch.SearchType.LESS_OR_EQUAL));
		assertEquals(1, partition1.indexOf(ts1, BinarySearch.SearchType.GREATER_OR_EQUAL));

		Partition<Quote> p = journal.openOrCreateLagPartition();
		long result = p.indexOf(Dates.toMillis("2012-06-15T00:00:00.000"), BinarySearch.SearchType.LESS_OR_EQUAL);
		assertEquals(-1, result);
	}

}