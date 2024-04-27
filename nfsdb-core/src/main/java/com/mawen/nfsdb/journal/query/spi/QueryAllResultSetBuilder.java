package com.mawen.nfsdb.journal.query.spi;

import java.util.List;

import com.mawen.nfsdb.journal.Partition;
import com.mawen.nfsdb.journal.UnorderedResultSetBuilder;
import com.mawen.nfsdb.journal.collections.IntArrayList;
import com.mawen.nfsdb.journal.collections.LongArrayList;
import com.mawen.nfsdb.journal.column.SymbolIndex;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.utils.Rows;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/27
 */
public class QueryAllResultSetBuilder<T> extends UnorderedResultSetBuilder<T> {

	private final IntArrayList symbolKeys;
	private final List<String> filterSymbols;
	private final IntArrayList filterSymbolKeys;
	private final String symbol;
	private SymbolIndex index;
	private SymbolIndex[] searchIndices;

	public QueryAllResultSetBuilder(Interval interval, String symbol, IntArrayList symbolKeys, List<String> filterSymbols, IntArrayList filterSymbolKeys) {
		super(interval);
		this.symbol = symbol;
		this.symbolKeys = symbolKeys;
		this.filterSymbols = filterSymbols;
		this.filterSymbolKeys = filterSymbolKeys;
	}

	@Override
	public Accept accept(Partition<T> partition) throws JournalException {
		super.accept(partition);
		this.index = partition.open().getIndexForColumn(symbol);

		// check if partition has at least one symbol value
		if (symbolKeys.size() > 0) {
			for (int i = 0; i < symbolKeys.size(); i++) {
				if (index.contains(symbolKeys.getQuick(i))) {
					searchIndices = new SymbolIndex[filterSymbols.size()];
					for (int k = 0; k < filterSymbols.size(); k++) {
						searchIndices[k] = partition.getIndexForColumn(filterSymbols.get(k));
					}
					return Accept.CONTINUE;
				}
			}
			return Accept.SKIP;
		}
		return Accept.BREAK;
	}

	@Override
	public void read(long lo, long hi) throws JournalException {
		for (int i = 0; i < symbolKeys.size(); i++) {
			int symbolKey = symbolKeys.getQuick(i);
			if (index.contains(symbolKey)) {
				if (searchIndices.length > 0) {
					for (int k = 0; k < searchIndices.length; k++) {
						if (searchIndices[k].contains(filterSymbolKeys.get(k))) {
							LongArrayList searchLocalRowIDs = searchIndices[k].getValues(filterSymbolKeys.get(k));
							LongArrayList symbolKeyRowIDs = index.getValues(symbolKey);
							for (int j = 0; j < symbolKeyRowIDs.size(); j++) {
								long localRowID = symbolKeyRowIDs.get(j);
								if (localRowID >= lo && localRowID < hi && searchLocalRowIDs.binarySearch(localRowID) >= 0) {
									result.add(Rows.toRowID(partition.getPartitionIndex(), localRowID));
								};
							}
						}
					}
				}
				else {
					LongArrayList symbolKeyRowIDs = index.getValues(symbolKey);
					int sz = symbolKeyRowIDs.size();
					result.setCapacity(sz);
					// optimise a bit
					if (symbolKeyRowIDs.get(0) >= lo && symbolKeyRowIDs.get(sz - 1) <= hi) {
						for (int j = 0; j < sz; j++) {
							result.add(Rows.toRowID(partition.getPartitionIndex(),symbolKeyRowIDs.getQuick(j)));
						}
					}
					else {
						for (int j = 0; j < sz; j++) {
							long localRowID = symbolKeyRowIDs.getQuick(j);
							if (localRowID >= lo && localRowID <= hi) {
								result.add(Rows.toRowID(partition.getPartitionIndex(), localRowID));
							}
						}
					}
				}
			}
		}
	}
}
