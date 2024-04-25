package com.mawen.nfsdb.journal.printer.converter;

import com.mawen.nfsdb.journal.printer.JournalPrinter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public abstract class AbstractConverter implements Converter{

	protected final JournalPrinter printer;

	public JournalPrinter getPrinter() {
		return printer;
	}

	protected AbstractConverter(JournalPrinter printer) {
		this.printer = printer;
	}
}
