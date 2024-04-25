package com.mawen.nfsdb.journal.printer.converter;

import com.mawen.nfsdb.journal.printer.JournalPrinter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class ObjectToStringConverter extends AbstractConverter{

	public ObjectToStringConverter(JournalPrinter printer) {
		super(printer);
	}

	@Override
	public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
		if (obj == null) {
			stringBuilder.append(getPrinter().getNullString());
		}
		else {
			stringBuilder.append(obj);
		}
	}
}
