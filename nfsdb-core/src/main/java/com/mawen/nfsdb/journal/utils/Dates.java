package com.mawen.nfsdb.journal.utils;

import com.mawen.nfsdb.journal.PartitionType;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.exceptions.JournalUnSupportedTypeException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public final class Dates {

	public static DateTime utc() {
		return DateTime.now(DateTimeZone.UTC);
	}

	public static DateTime utc(String date) {
		return new DateTime(date, DateTimeZone.UTC);
	}

	public static DateTime utc(int year, int month, int day, int hour, int minute) {
		return new DateTime(year, month, day, hour, minute, DateTimeZone.UTC);
	}

	public static DateTime utc(long millis) {
		return new DateTime(millis, DateTimeZone.UTC);
	}

	public static String toString(long millis) {
		return utc(millis).toString();
	}

	public static long toMillis(String date) {
		return new DateTime(date, DateTimeZone.UTC).getMillis();
	}

	public static Interval interval(String start, String end) {
		return interval(toMillis(start), toMillis(end));
	}

	public static Interval interval(long start, long end) {
		if (end < start) {
			return new Interval(end, start, DateTimeZone.UTC);
		}
		else {
			return new Interval(start, end, DateTimeZone.UTC);
		}
	}

	public static Interval lastMonths(int duration) {
		return lastMonths(utc(), duration);
	}

	public static Interval intervalForDirName(String name, PartitionType partitionType) {
		switch (partitionType) {
			case YEAR:
				return intervalForDate(Dates.utc(name + "-01-01T00:00:00.000Z").getMillis(), partitionType);
			case MONTH:
				return intervalForDate(Dates.utc(name + "-01T00:00:00.000Z").getMillis(), partitionType);
			case DAY:
				return intervalForDate(Dates.utc(name + "T00:00:00.000Z").getMillis(), partitionType);
			case NONE:
				if ("default".equals(name)) {
					break;
				}
			default:
				throw new JournalUnSupportedTypeException(partitionType);
		}
		return null;
	}

	public static Interval intervalForDate(long timestamp, PartitionType partitionType) {
		long lo = intervalStart(timestamp, partitionType);
		long hi = intervalEnd(lo, partitionType);

		switch (partitionType) {
			case YEAR:
				return null;
			default:
				return new Interval(lo, hi, DateTimeZone.UTC);
		}
	}

	public static String dirNameForIntervalStart(Interval interval, PartitionType partitionType) {
		switch (partitionType) {
			case YEAR:
				return interval.getStart().toString("YYYY");
			case MONTH:
				return interval.getStart().toString("YYYY-MM");
			case DAY:
				return interval.getStart().toString("YYYY-MM-DD");
			case NONE:
				return "default";
		}
		return "";
	}

	/////////////////////////////////////////////////////////////////

	private static Interval lastMonths(DateTime endDateTime, int duration) {
		if (duration < 1) {
			throw new JournalRuntimeException("Duration should be >= 1: %d", duration);
		}
		DateTime start = endDateTime.minusMonths(duration);
		return new Interval(start, endDateTime);
	}

	private static long intervalStart(long timestamp, PartitionType partitionType) {
		switch (partitionType) {
			case YEAR:
				return Dates.utc(timestamp).withMonthOfYear(1).withDayOfMonth(1).withTimeAtStartOfDay().getMillis();
			case MONTH:
				return Dates.utc(timestamp).withDayOfMonth(1).withTimeAtStartOfDay().getMillis();
			case DAY:
				return Dates.utc(timestamp).withTimeAtStartOfDay().getMillis();
		}
		return 0;
	}

	private static long intervalEnd(long start, PartitionType partitionType) {
		switch (partitionType) {
			case YEAR:
				return Dates.utc(start).plusYears(1).getMillis() - 1;
			case MONTH:
				return Dates.utc(start).plusMonths(1).getMillis() - 1;
			case DAY:
				return Dates.utc(start).plusDays(1).getMillis() - 1;
		}
		return 0;
	}

	private Dates () {}
}
