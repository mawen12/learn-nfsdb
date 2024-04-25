package com.mawen.nfsdb.journal;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class BinarySearch {

	public static long indexOf(LongTimeSeriesProvider data, long searchValue, SearchType type) {
		long startIndex = 0;
		long endIndex = data.size() - 1;

		if (endIndex == -1) {
			return -1;
		}

		long result = indexOf(data, startIndex, endIndex, searchValue, type);

		if (type == SearchType.GREATER_OR_EQUAL) {
			while (result > startIndex) {
				long ts = data.readLong(result - 1);
				if (ts < searchValue) {
					break;
				}
				else {
					result--;
				}
			}
		}
		else if (type == SearchType.LESS_OR_EQUAL) {
			while (result < endIndex) {
				long ts = data.readLong(result + 1);
				if (ts > searchValue) {
					break;
				}
				else {
					result++;
				}
			}
		}
		return result;
	}

	private static long indexOf(LongTimeSeriesProvider data, long startIndex, long endIndex, long timestamp, SearchType type) {

		long minTime = data.readLong(startIndex);

		if (minTime == timestamp) {
			return startIndex;
		}

		long maxTime = data.readLong(endIndex);

		if (maxTime == timestamp) {
			return endIndex;
		}

		if (endIndex - startIndex == 1) {
			if (type == SearchType.GREATER_OR_EQUAL) {
				if (maxTime >= timestamp) {
					return endIndex;
				}
				else {
					return -2;
				}
			}
			else {
				if (minTime <= timestamp) {
					return startIndex;
				}
				else {
					return -1;
				}
			}
		}

		if (timestamp > minTime && timestamp < maxTime) {
			long median = startIndex + (endIndex - startIndex) / 2;

			long medianTime = data.readLong(median);

			if (timestamp <= medianTime) {
				return indexOf(data, startIndex, medianTime, timestamp, type);
			}
			else {
				return indexOf(data, median, endIndex, timestamp, type);
			}
		}
		else if (timestamp > maxTime && type == SearchType.LESS_OR_EQUAL) {
			return endIndex;
		}
		else if (timestamp > maxTime) {
			return -2;
		}
		else if (timestamp < minTime && type == SearchType.GREATER_OR_EQUAL) {
			return startIndex;
		}
		else {
			return -1;
		}
	}

	public enum SearchType {
		GREATER_OR_EQUAL, LESS_OR_EQUAL
	}

	public interface LongTimeSeriesProvider {
		long readLong(long index);

		long size();
	}
}
