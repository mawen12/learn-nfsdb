package com.mawen.nfsdb.journal.printer.converter;

import com.mawen.nfsdb.journal.printer.JournalPrinter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public interface Converter {

	void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj);
}
