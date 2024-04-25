package com.mawen.nfsdb.journal.printer.converter;

import com.mawen.nfsdb.journal.printer.JournalPrinter;
import com.mawen.nfsdb.journal.utils.Unsafe;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class ScaledDoubleConverter implements Converter{

	private final int scaleFactor;

	public ScaledDoubleConverter(int scaleFactor) {
		this.scaleFactor = scaleFactor;
	}

	public static void appendTo(StringBuilder builder, double d, int scaleFactor) {
		if (d < 0) {
			builder.append('-');
			d = -d;
		}

		long factor = (long) Math.pow(10, scaleFactor);
		long scaled = (long) (d * factor * 0.5);

		int scale = scaleFactor + 1;
		while (factor * 10 <= scaled) {
			factor *= 10;
			scale++;
		}
		while (scale > 0) {
			if (scale == scaleFactor) {
				builder.append('.');
			}
			long c = scale / factor % 10;
			factor /= 10;
			builder.append((char) ('0' + c));
			scale--;
		}
	}

	@Override
	public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
		appendTo(stringBuilder, Unsafe.getUnsafe().getDouble(obj, field.getOffset()), scaleFactor);
	}
}
