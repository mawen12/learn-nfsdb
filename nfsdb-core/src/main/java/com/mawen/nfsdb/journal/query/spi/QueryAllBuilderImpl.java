package com.mawen.nfsdb.journal.query.spi;

import java.util.ArrayList;
import java.util.List;

import com.mawen.nfsdb.journal.Journal;
import com.mawen.nfsdb.journal.UnorderedResultSet;
import com.mawen.nfsdb.journal.collections.IntArrayList;
import com.mawen.nfsdb.journal.column.SymbolTable;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.query.api.QueryAllBuilder;
import org.joda.time.Interval;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/27
 */
public class QueryAllBuilderImpl<T> implements QueryAllBuilder<T> {

	private final Journal<T> journal;
	private final IntArrayList symbolKeys = new IntArrayList();
	private final List<String> filterSymbols = new ArrayList<>();
	private final IntArrayList filterSymbolKeys = new IntArrayList();
	private String symbol;
	private Interval interval;

	public QueryAllBuilderImpl(Journal<T> journal) {
		this.journal = journal;
	}

	@Override
	public QueryAllBuilder<T> limit(Interval interval) {
		setInterval(interval);
		return this;
	}

	@Override
	public UnorderedResultSet<T> asResultSet() throws JournalException {
		return journal.iteratePartitions(
				new QueryAllResultSetBuilder<T>(interval, symbol, symbolKeys, filterSymbols, filterSymbolKeys));
	}

	public void setSymbol(String symbol, String... values) {
		this.symbol = symbol;
		SymbolTable symbolTable = journal.getSymbolTable(symbol);
		this.symbolKeys.resetQuick();
		for (String value : values) {
			symbolKeys.add(symbolTable.get(value));
		}
	}

	@Override
	public QueryAllBuilder<T> filter(String symbol, String value) {
		SymbolTable tab = journal.getSymbolTable(symbol);
		int key = tab.get(value);
		filterSymbols.add(symbol);
		filterSymbolKeys.add(key);
		return this;
	}

	@Override
	public void resetFilter() {
		filterSymbols.clear();
		filterSymbolKeys.resetQuick();
	}

	public void setInterval(Interval interval) {
		this.interval = interval;
	}
}
