package com.mawen.nfsdb.journal.printer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.printer.appender.Appender;
import com.mawen.nfsdb.journal.printer.appender.StdOutAppender;
import com.mawen.nfsdb.journal.printer.converter.BooleanConverter;
import com.mawen.nfsdb.journal.printer.converter.ByteConverter;
import com.mawen.nfsdb.journal.printer.converter.Converter;
import com.mawen.nfsdb.journal.printer.converter.DoubleConverter;
import com.mawen.nfsdb.journal.printer.converter.IntConverter;
import com.mawen.nfsdb.journal.printer.converter.LongConverter;
import com.mawen.nfsdb.journal.printer.converter.ObjectToStringConverter;
import com.mawen.nfsdb.journal.printer.converter.StringConverter;
import com.mawen.nfsdb.journal.utils.Unsafe;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class JournalPrinter implements Closeable {

	private static final long OBJECT_VALUE_OFFSET = -1;
	private final StringBuilder rowBuilder = new StringBuilder();
	private final List<Field> ff = new ArrayList<>();
	private Class[] typeTemplate = new Class[] {};
	private String delimiter = "\t";
	private Appender appender = StdOutAppender.INSTANCE;
	private String nullString;
	private boolean configured = false;

	public JournalPrinter() {
	}

	public Field f(String name) {
		Field field = new Field(name, this);
		ff.add(field);
		return field;
	}

	public Field v(int typeIndex) {
		Field field = new Field(typeIndex, this);
		ff.add(field);
		return field;
	}

	public JournalPrinter types(Class... clazz) {
		typeTemplate = clazz;
		return this;
	}

	public JournalPrinter setDelimiter(String delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	public String getNullString() {
		return nullString;
	}

	public void setNullString(String nullString) {
		this.nullString = nullString;
	}

	public JournalPrinter setAppender(Appender appender) {
		this.appender = appender;
		return this;
	}

	public void out(Object... instances) throws IOException {
		configure();
		rowBuilder.setLength(0);
		for (int i = 0; i < ff.size(); i++) {
			if (i > 0) {
				rowBuilder.append(delimiter);
			}
			Field f = ff.get(i);
			Object instance = instances[f.typeTemplateIndex];
			if (instance != null) {
				f.converter.convert(rowBuilder, f, instances[f.typeTemplateIndex]);
			}
		}
		appender.append(rowBuilder);
	}

	public void configure() {
		if (configured) {
			return;
		}

		try {
			for (Field f : ff) {
				// value field
				if (f.name == null) {
					f.fromType = getType(f.typeTemplateIndex);
					f.offset = OBJECT_VALUE_OFFSET;
				}
				else if (f.typeTemplateIndex == -1) {
					// reference field without explicit type index

					// find which type contains this field name
					// first type in typeTemplate array wins
					for (int i = 0; i < typeTemplate.length; i++) {
						Class clazz = typeTemplate[i];
						for (java.lang.reflect.Field field : clazz.getFields()) {
							if (f.name.equals(field.getName())) {
								f.fromType = field.getType();
								f.typeTemplateIndex = i;
								break;
							}
						}
						// found type
						if (f.typeTemplateIndex != -1) {
							break;
						}
					}

					// finish loop without finding type
					if (f.typeTemplateIndex == -1) {
						throw new RuntimeException("No such field: " + f.name);
					}
					f.offset = Unsafe.getUnsafe().objectFieldOffset(getType(f.typeTemplateIndex).getField(f.name));
				}
				else {
					// reference field with known type template index
					Class t = getType(f.typeTemplateIndex);
					f.fromType = t.getField(f.name).getType();
					f.offset = Unsafe.getUnsafe().objectFieldOffset(t.getField(f.name));
				}

				setConverter(f);
			}
			configured = true;
		}
		catch (NoSuchFieldException e) {
			throw new JournalRuntimeException(e);
		}
	}

	public void head() throws IOException {
		rowBuilder.setLength(0);
		for (int i = 0; i < ff.size(); i++) {
			if (i > 0) {
				rowBuilder.append(delimiter);
			}

			Field f = ff.get(i);
			String header = f.header;
			if (header == null) {
				rowBuilder.append(f.name);
			}
			else {
				rowBuilder.append(f.header);
			}
		}
		appender.append(rowBuilder);
	}

	@Override
	public void close() throws IOException {
		appender.close();
		ff.clear();
		configured = false;
	}

	private Class getType(int typeIndex) {
		if (typeIndex < 0 || typeIndex >= typeTemplate.length) {
			throw new JournalRuntimeException("Invalid index: %d", typeIndex);
		}
		return typeTemplate[typeIndex];
	}

	private void setConverter(Field f) {
		if (f.converter == null) {
			if (f.offset == OBJECT_VALUE_OFFSET) {
				f.converter = new ObjectToStringConverter(this);
			}
			else if (f.fromType == int.class) {
				f.converter = new IntConverter();
			}
			else if (f.fromType == long.class) {
				f.converter = new LongConverter();
			}
			else if (f.fromType == double.class) {
				f.converter = new DoubleConverter();
			}
			else if (f.fromType == boolean.class) {
				f.converter = new BooleanConverter();
			}
			else if (f.fromType == byte.class) {
				f.converter = new ByteConverter();
			}
			else if (f.fromType == String.class) {
				f.converter = new StringConverter(this);
			}
		}
	}

	public static class Field {
		private final JournalPrinter printer;
		private String header;
		private String name;
		private int typeTemplateIndex = -1;
		private long offset;
		private Class fromType;
		private Converter converter;

		public Field(String name, JournalPrinter printer) {
			this.name = name;
			this.printer = printer;
		}

		public Field(int typeTemplateIndex, JournalPrinter printer) {
			this.typeTemplateIndex = typeTemplateIndex;
			this.printer = printer;
		}

		public Field h(String header) {
			this.header = header;
			return this;
		}

		public Field i(int instanceIndex) {
			this.typeTemplateIndex = instanceIndex;
			return this;
		}

		public Field f(String name) {
			return printer.f(name);
		}

		public Field v(int typeIndex) {
			return printer.v(typeIndex);
		}

		public Field c(Converter converter) {
			this.converter = converter;
			return this;
		}

		public long getOffset() {
			return offset;
		}
	}
}
