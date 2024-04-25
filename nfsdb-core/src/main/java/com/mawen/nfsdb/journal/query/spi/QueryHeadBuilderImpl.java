package com.mawen.nfsdb.journal.query.spi;

import java.util.ArrayList;
import java.util.List;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.collections.IntArrayList;
import com.mawen.nfsdb.journal.column.SymbolTable;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.query.api.QueryHeadBuilder;
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

	}
}
