package com.mawen.nfsdb.journal.query.spi;

import java.util.ArrayList;
import java.util.List;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.Partition;
import com.mawen.nfsdb.journal.UnorderedResultSet;
import com.mawen.nfsdb.journal.UnorderedResultSetBuilder;
import com.mawen.nfsdb.journal.collections.IntArrayList;
import com.mawen.nfsdb.journal.collections.LongArrayList;
import com.mawen.nfsdb.journal.column.SymbolIndex;
import com.mawen.nfsdb.journal.column.SymbolTable;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.query.api.QueryHeadBuilder;
import com.mawen.nfsdb.journal.utils.Rows;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class QueryHeadBuilderImpl<T> implements QueryHeadBuilder<T> {

	private final Journal<T> journal;
	private final IntArrayList symbolKeys = new IntArrayList();
	private final List<String> filterSymbols = new ArrayList<>();
	private final IntArrayList filterSymbolKeys = new IntArrayList();
	private int symbolColumnIndex;
	private Interval interval;
	private long minRowID = -1L;
	private boolean strict = true;

	public QueryHeadBuilderImpl(Journal<T> journal) {
		this.journal = journal;
	}

	public void setSymbol(String symbol, String... values) {
		this.symbolColumnIndex = journal.getMetadata().getColumnIndex(symbol);
		SymbolTable symbolTable = journal.getSymbolTable(symbol);
		this.symbolKeys.resetQuick();
		this.symbolKeys.ensureCapacity(values == null || values.length == 0 ? symbolTable.size() : values.length);
		if (values == null || values.length == 0) {
			this.symbolKeys.ensureCapacity(symbolTable.size());
			for (int i = 0; i < symbolTable.size(); i++) {
				this.symbolKeys.add(i);
			}
		}
		else {
			this.symbolKeys.ensureCapacity(values.length);
			for (String value : values) {
				symbolKeys.add(symbolTable.get(value));
			}
		}
	}

	@Override
	public QueryHeadBuilder<T> limit(Interval interval) {
		this.interval = interval;
		this.minRowID = -1L;
		return this;
	}

	@Override
	public QueryHeadBuilder<T> limit(long minRowID) {
		this.minRowID = minRowID;
		this.interval = null;
		return this;
	}

	@Override
	public QueryHeadBuilder<T> filter(String symbol, String value) {
		SymbolTable tab = journal.getSymbolTable(symbol);
		int key = tab.get(value);
		filterSymbols.add(symbol);
		filterSymbolKeys.add(key);
		return this;
	}

	@Override
	public QueryHeadBuilder<T> strict(boolean strict) {
		this.strict = strict;
		return this;
	}

	@Override
	public void resetFilter() {
		filterSymbols.clear();
		filterSymbolKeys.resetQuick();
	}

	@Override
	public UnorderedResultSet<T> asResultSet() throws JournalException {
		final int minPartitionIndex;
		final long minLocalRowID;

		if (minRowID == -1) {
			minPartitionIndex = 0;
			minLocalRowID = -1L;
		}
		else {
			minPartitionIndex = Rows.toPartitionIndex(minRowID);
			minLocalRowID = Rows.toLocalRowID(minRowID);
		}

		final IntArrayList symbolKeys = new IntArrayList(this.symbolKeys);

		return journal.iteratePartitions(new UnorderedResultSetBuilder<T>(interval) {
			private final SymbolIndex[] filterSymbolIndexes = new SymbolIndex[filterSymbolKeys.size()];
			private final LongArrayList[] filterSymbolRows = new LongArrayList[filterSymbolKeys.size()];
			private IntArrayList keys = new IntArrayList();
			private IntArrayList remainingKeys = new IntArrayList(keys.size());

			{
				for (int i = 0; i < filterSymbolRows.length; i++) {
					filterSymbolRows[i] = new LongArrayList();
				}
			}

			@Override
			public Accept accept(Partition<T> partition) throws JournalException {
				super.accept(partition);
				return keys.isEmpty() || partition.getPartitionIndex() < minPartitionIndex ? Accept.BREAK : Accept.CONTINUE;
			}

			@Override
			public void read(long lo, long hi) throws JournalException {
				SymbolIndex index = partition.getIndexForColumn(symbolColumnIndex);

				boolean filterOk = true;
				for (int i = 0; i < filterSymbols.size(); i++) {
					filterSymbolIndexes[i] = partition.getIndexForColumn(filterSymbols.get(i));
					int filterKey = filterSymbolKeys.getQuick(i);
					if (filterSymbolIndexes[i].contains(filterKey)) {
						filterSymbolIndexes[i].getValues(filterKey, filterSymbolRows[i]);
					}
					else {
						filterOk = false;
						break;
					}
				}

				if (filterOk) {
					for (int k = 0; k < keys.size(); k++) {
						int key = keys.getQuick(k);
						boolean found = false;
						NEXT_KEY:
						for (int i = index.getValueCount(key) - 1; i >= 0; i--) {
							long localRowID = index.getValueQuick(key, i);
							if (localRowID <= hi && localRowID >= lo
							    && (partition.getPartitionIndex() > minPartitionIndex || localRowID > minLocalRowID)) {

								boolean matches = true;
								for (LongArrayList filterRows : filterSymbolRows) {
									if (filterRows.binarySearch(localRowID) > 0) {
										matches = false;
										if (strict) {
											found = true;
											break NEXT_KEY;
										}
										else {
											break;
										}
									}
								}

								if (matches) {
									result.add(Rows.toRowID(partition.getPartitionIndex(), localRowID));
									found = true;
									break;
								}
							}
							else if (localRowID < lo || (partition.getPartitionIndex() <= minPartitionIndex && localRowID <= minLocalRowID)) {
								// localRowID is only going to get lower, so fail fast
								found = true;
								break;
							}
						}

						if (!found) {
							remainingKeys.add(key);
						}
					}
					IntArrayList temp = keys;
					keys = remainingKeys;
					remainingKeys = temp;
					remainingKeys.resetQuick();
				}
			}
		});
	}
}
