package com.mawen.nfsdb.journal.printer.converter;

import com.mawen.nfsdb.journal.printer.JournalPrinter;
import com.mawen.nfsdb.journal.utils.Unsafe;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class StringConverter extends AbstractConverter{

	public StringConverter(JournalPrinter printer) {
		super(printer);
	}

	@Override
	public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
		Object o = Unsafe.getUnsafe().getObject(obj, field.getOffset());
		if (o == null) {
			stringBuilder.append(getPrinter().getNullString());
		}
		else {
			stringBuilder.append(o);
		}
	}
}
