package com.mawen.nfsdb.journal.printer.converter;

import com.mawen.nfsdb.journal.printer.JournalPrinter;
import com.mawen.nfsdb.journal.utils.Unsafe;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class DateConverter extends AbstractConverter{

	private final DateTimeFormatter formatter;

	public DateConverter(JournalPrinter printer) {
		this(printer, ISODateTimeFormat.dateTime().withZoneUTC());
	}

	public DateConverter(JournalPrinter printer, DateTimeFormatter formatter) {
		super(printer);
		this.formatter = formatter;
	}

	@Override
	public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
		final long millis = Unsafe.getUnsafe().getLong(obj, field.getOffset());
		if (millis == 0) {
			stringBuilder.append(getPrinter().getNullString());
		}
		else {
			stringBuilder.append(formatter.print(millis));
		}
	}
}
