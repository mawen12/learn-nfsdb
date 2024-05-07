package com.mawen.nfsdb.journal;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

import com.mawen.nfsdb.journal.collections.LongArrayList;
import com.mawen.nfsdb.journal.column.SymbolIndex;
import com.mawen.nfsdb.journal.column.SymbolTable;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.factory.JournalMetadata;
import com.mawen.nfsdb.journal.iterators.JournalIterator;
import com.mawen.nfsdb.journal.printer.JournalPrinter;
import com.mawen.nfsdb.journal.printer.appender.AssertingAppender;
import com.mawen.nfsdb.journal.printer.converter.DateConverter;
import com.mawen.nfsdb.journal.test.model.Quote;
import com.mawen.nfsdb.journal.test.model.TestEntity;
import com.mawen.nfsdb.journal.utils.Dates;
import com.mawen.nfsdb.journal.utils.Unsafe;
import gnu.trove.map.hash.TIntIntHashMap;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/7
 */
public class TestUtils {


	public static <T> void assertEquals(String expected, ResultSet<T> rs) throws IOException {
		assertEquals(expected, rs.bufferedIterator());
	}

	public static <T> void assertEquals(String expected, JournalIterator<T> actual) throws IOException {
		try (JournalPrinter p = new JournalPrinter()) {
			p.setAppender(new AssertingAppender(expected));
			configure(p, actual.getJournal().getMetadata());
			out(p, actual);
		}
	}

	public static <T> void print(JournalIterator<T> iterator) throws IOException {
		try (JournalPrinter p = new JournalPrinter()) {
			configure(p, iterator.getJournal().getMetadata());
			out(p, iterator);
		}
	}

	public static <T> void assertOrder(ResultSet<T> rs) {
		assertOrder(rs.bufferedIterator());
	}

	public static <T> void assertOrder(JournalIterator<T> rs) {
		JournalMetadata.ColumnMetadata meta = rs.getJournal().getMetadata().getTimestampColumnMetadata();
		long max = 0;
		for (T obj : rs) {
			long timestamp = Unsafe.getUnsafe().getLong(obj, meta.offset);
			if (timestamp < max) {
				throw new AssertionError("Timestamp out of order. [ " + Dates.toString(timestamp) + " < " + Dates.toString(max) + "]");
			}
			max = timestamp;
		}
	}

	public static void configure(JournalPrinter p, JournalMetadata meta) {
		p.types(meta.getModelClass());

		for (int i = 0; i < meta.getColumnCount(); i++) {
			String name = meta.getColumnMetadata(i).name;
			if (!"__isset_bit_vector".equals(name) && !"__isset_bitfield".equals(name)) {
				JournalPrinter.Field f = p.f(name);
				if (i == meta.getTimestampColumnIndex()) {
					f.c(new DateConverter(p));
				}
			}
		}
	}

	public static void generateQuoteData(JournalWriter<Quote> w, int count, long timestamp) throws JournalException {
		generateQuoteData(w, count, timestamp, 0);
	}

	public static void generateQuoteData(JournalWriter<Quote> w, int count) throws JournalException {
		String[] symbols = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
		long[] timestamps = {Dates.toMillis("2023-09-04T10:00:00.000Z"), Dates.toMillis("2023-10-04T10:00:00.000Z"), Dates.toMillis("2023-11-04T10:00:00.000Z")};
		Quote q = new Quote();
		Random r = new Random(System.currentTimeMillis());
		for (int i = 0; i < count; i++) {
			w.clearObject(q);
			q.setSym(symbols[Math.abs(r.nextInt() % symbols.length)]);
			q.setAsk(Math.abs(r.nextDouble()));
			q.setBid(Math.abs(r.nextDouble()));
			q.setAskSize(Math.abs(r.nextInt()));
			q.setBidSize(Math.abs(r.nextInt()));
			q.setEx("LXE");
			q.setMode("Fast trading");
			q.setTimestamp(timestamps[i * timestamps.length / count]);
			w.append(q);
		}
		w.commit();
	}

	public static void generateQuoteData(JournalWriter<Quote> w, int count, long timestamp, int increment) throws JournalException {
		String[] symbols = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
		Quote q = new Quote();
		Random r = new Random(System.currentTimeMillis());
		for (int i = 0; i < count; i++) {
			w.clearObject(q);
			q.setSym(symbols[Math.abs(r.nextInt() % symbols.length)]);
			q.setAsk(Math.abs(r.nextDouble()));
			q.setBid(Math.abs(r.nextDouble()));
			q.setAskSize(Math.abs(r.nextInt()));
			q.setBidSize(Math.abs(r.nextInt()));
			q.setEx("LXE");
			q.setMode("Fast trading");
			q.setTimestamp(timestamp);
			timestamp += increment;
			w.append(q);
		}
	}

	public static void generateQuoteData(JournalWriter<Quote> w, int count, Interval interval) throws JournalException {
		generateQuoteData(w, count, interval, true);
	}

	public static void generateQuoteData(JournalWriter<Quote> w, int count, Interval interval, boolean commit) throws JournalException {
		int[] increment = new int[interval.toPeriod().getMonths() + 1];
		DateTime start = interval.getStart();

		int perMonth = count / increment.length;

		for (int i = 0; i < increment.length; i++) {
			DateTime end = start.dayOfMonth().withMaximumValue();
			if (end.getMonthOfYear() == interval.getEnd().getMonthOfYear()) {
				end = interval.getEnd();
			}

			long delta = end.getMillis() - start.getMillis();
			if (delta < perMonth) {
				throw new JournalRuntimeException("Pick interval with start and end dates farther away from month edges");
			}

			increment[i] = (int) (delta / perMonth);
			String[] symbols = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
			Quote q = new Quote();
			Random r = new Random(System.currentTimeMillis());

			for (int j = 0; j < perMonth; j++) {
				w.clearObject(q);
				q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length - 1))]);
				q.setAsk(Math.abs(r.nextDouble()));
				q.setBid(Math.abs(r.nextDouble()));
				q.setAskSize(Math.abs(r.nextInt()));
				q.setBidSize(Math.abs(r.nextInt()));
				q.setEx("LXE");
				q.setMode("Fast trading");
				q.setTimestamp(start.getMillis());
				w.append(q);
				start.plusMillis(increment[i]);
			}
			start = end.plusDays(1);
		}

		if (commit) {
			w.commit();
		}
	}


	public static void generateQuoteData(int count, long timestamp, int increment) {
		String[] symbols = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
		String[] exchanges = {"LXE", "GR", "SK", "LN"};
		Random r = new Random(System.currentTimeMillis());
		for (int i = 0; i < count; i++) {
			Quote q = new Quote();
			q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length - 1))]);
			q.setAsk(Math.abs(r.nextDouble()));
			q.setBid(Math.abs(r.nextDouble()));
			q.setAskSize(Math.abs(r.nextInt()));
			q.setBidSize(Math.abs(r.nextInt()));
			q.setEx(exchanges[Math.abs(r.nextInt() % (exchanges.length - 1))]);
			q.setMode("Fast trading");
			q.setTimestamp(timestamp);
			timestamp += increment;
			print(q);
		}
	}

	public static void generateQuoteData2(int count, long timestamp, int increment) {
		String[] symbols = {"ALDW", "AMD", "HSBA.L"};
		String[] exchanges = {"LXE", "GR", "SK", "LN"};
		Random r = new Random(System.currentTimeMillis());
		for (int i = 0; i < count; i++) {
			Quote q = new Quote();
			q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length))]);
			q.setAsk(Math.abs(r.nextDouble()));
			q.setBid(Math.abs(r.nextDouble()));
			q.setAskSize(Math.abs(r.nextInt()));
			q.setBidSize(Math.abs(r.nextInt()));
			q.setEx(exchanges[Math.abs(r.nextInt() % (exchanges.length))]);
			q.setMode("Fast trading");
			q.setTimestamp(timestamp);
			timestamp += increment;
			print(q);
		}
	}


	public static void generateTestEntityData(JournalWriter<TestEntity> w, int count) throws JournalException {
		generateTestEntityData(w, count, Dates.toMillis("2024-05-15T10:55:00.000Z"), count * 100);
	}

	public static void generateTestEntityData(JournalWriter<TestEntity> w, int count, long timestamp, int increment) throws JournalException {
		String[] symbols = {"AGk.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L", null};
		Random r = new Random(System.currentTimeMillis());
		for (int i = 0; i < count; i++) {
			TestEntity e = new TestEntity();
			e.setSym(symbols[Math.abs(r.nextInt() % symbols.length)]);
			e.setAnInt(Math.abs(r.nextInt()));
			e.setADouble(Math.abs(r.nextDouble()));
			e.setBStr(UUID.randomUUID().toString());
			e.setDStr(UUID.randomUUID().toString());
			e.setDwStr(UUID.randomUUID().toString());
			e.setTimestamp(timestamp);
			timestamp += increment;
			w.append(e);
		}
		w.commit();
	}

	public static <T> void assertDataEquals(Journal<T> expected, Journal<T> actual) throws JournalException {
		Assert.assertEquals(expected.size(), actual.size());
		OrderedResultSet<T> er = expected.query().all().asResultSet();
		OrderedResultSet<T> ar = actual.query().all().asResultSet();
		for (int i = 0; i < er.size(); i++) {
			Assert.assertEquals(er.read(i), ar.read(i));
		}
	}

	public static <T> void assertEquals(Journal<T> expected, Journal<T> actual) throws JournalException {
		Assert.assertEquals(expected.size(), actual.size());
		Assert.assertEquals(expected.getPartitionCount(), actual.getPartitionCount());

		// check if SymbolIndexes are the same

		TIntIntHashMap colKeyCount = new TIntIntHashMap();
		for (int i = 0; i < expected.getMetadata().getColumnCount(); i++) {
			SymbolTable et = expected.getColumnMetadata(i).symbolTable;
			SymbolTable at = actual.getColumnMetadata(i).symbolTable;

			if (et == null && at == null) {
				continue;
			}

			if (et == null || at == null) {
				Assert.fail("SymbolTable mismatch");
			}

			Assert.assertEquals(et.size(), at.size());

			colKeyCount.put(i, et.size());

			for (int j = 0; j < et.size(); j++) {
				String ev = et.value(j);
				String av = at.value(j);
				Assert.assertEquals(ev, av);
				Assert.assertEquals(et.getQuick(ev), at.getQuick(av));
			}
		}

		// check if partitions are the same
		for (int i = 0; i < expected.getPartitionCount(); i++) {
			Partition<T> ep = expected.getPartition(i, true);
			Partition<T> ap = actual.getPartition(i, true);

			// compare names
			Assert.assertEquals(ep.getName(), ap.getName());
			// compare sizes
			Assert.assertEquals(ep.size(), ap.size());
			// compare intervals
			if (ep != expected.getIrregularPartition() || ap != actual.getIrregularPartition()) {
				Assert.assertEquals("Interval mismatch. partition=" + i, ep.getInterval(), ap.getInterval());
			}

			for (int j = 0; j < expected.getMetadata().getColumnCount(); j++) {
				if (expected.getColumnMetadata(j).meta.indexed) {
					SymbolIndex ei = ep.getIndexForColumn(j);
					SymbolIndex ai = ap.getIndexForColumn(j);

					int count = colKeyCount.get(j);

					for (int k = 0; k < count; k++) {
						LongArrayList ev = ei.getValues(j);
						LongArrayList av = ai.getValues(j);

						Assert.assertEquals("Values mismatch. partition=" + i + ", column=" + expected.getColumnMetadata(j).meta.name + ", key=" + k + ": ", ev.size(), av.size());
						for (int l = 0; l < ev.size(); l++) {
							Assert.assertEquals(ev.get(l), av.get(l));
						}
					}
				}
			}

			for (int j = 0; j < ep.size(); j++) {
				Assert.assertEquals(ep.read(j), ap.read(j));
			}
		}
	}

	public static <T> void assertEquals(Iterator<T> expected, Iterator<T> actual) {
		while (true) {
			boolean expectedHasNext = expected.hasNext();
			boolean actualHasNext = actual.hasNext();

			Assert.assertEquals(expectedHasNext, actualHasNext);

			if (!expectedHasNext) {
				break;
			}

			Assert.assertEquals(expected.next(), actual.next());
		}
	}

	private static <T> void out(JournalPrinter p, JournalIterator<T> iterator) throws IOException {
		for (T o : iterator) {
			p.out(o);
		}
	}

	private static void print(Quote q) {
		StringBuilder sb = new StringBuilder();
		sb.append("w.append(");
		sb.append("new Quote()");
		sb.append(".setSym(\"").append(q.getSym()).append("\")");
		sb.append(".setAsk(").append(q.getAsk()).append(")");
		sb.append(".setBid(").append(q.getBid()).append(")");
		sb.append(".setAskSize(").append(q.getAskSize()).append(")");
		sb.append(".setBidSize(").append(q.getBidSize()).append(")");
		sb.append(".setEx(\"").append(q.getEx()).append("\")");
		sb.append(".setMode(\"").append(q.getMode()).append("\")");
		sb.append(".setTimestamp(Dates.toMillis(\"").append(Dates.toString(q.getTimestamp())).append("\")");
		sb.append(")");
		sb.append(";");
		System.out.println(sb);
	}

	private TestUtils() {
	}
}
