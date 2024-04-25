package com.mawen.nfsdb.journal.printer.converter;

import com.mawen.nfsdb.journal.printer.JournalPrinter;
import com.mawen.nfsdb.journal.utils.Unsafe;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class ByteConverter implements Converter{

	@Override
	public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
		stringBuilder.append(Unsafe.getUnsafe().getByte(obj, field.getOffset()));
	}
}
