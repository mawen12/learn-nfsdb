package com.mawen.nfsdb.journal.printer.converter;

import java.util.regex.Pattern;

import com.mawen.nfsdb.journal.printer.JournalPrinter;
import com.mawen.nfsdb.journal.utils.Unsafe;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class StripCRLFStringConverter extends AbstractConverter {

	private static final Pattern CR = Pattern.compile("\n", Pattern.LITERAL);
	private static final Pattern LF = Pattern.compile("\r", Pattern.LITERAL);

	public StripCRLFStringConverter(JournalPrinter printer) {
		super(printer);
	}

	@Override
	public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
		String s;

		if (field.getOffset() == -1) {
			s = obj.toString();
		}
		else {
			s = (String) Unsafe.getUnsafe().getObject(obj, field.getOffset());
		}

		if (s == null) {
			stringBuilder.append(getPrinter().getNullString());
		}
		else {
			stringBuilder.append(LF.matcher(CR.matcher(s).replaceAll(" ")).replaceAll(""));
		}
	}
}
