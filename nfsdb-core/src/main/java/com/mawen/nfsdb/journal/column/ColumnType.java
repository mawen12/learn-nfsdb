package com.mawen.nfsdb.journal.column;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public enum ColumnType {
	BOOLEAN(boolean.class, 1, true),
	BYTE(byte.class, 1, true),
	SHORT(short.class, 2, true),
	INT(int.class, 4, true),
	LONG(long.class, 8, true),
	DOUBLE(double.class, 8, true),
	STRING(String.class, 0, false),
	SYMBOL(null, 4, false),
	;

	private final Class<?> type;
	private final boolean primitive;
	private final int size;

	public boolean matches(Class<?> type) {
		return this.type == type;
	}

	public boolean primitive() {
		return primitive;
	}

	public int size() {
		return size;
	}

	ColumnType(Class<?> type, int size, boolean primitive) {
		this.type = type;
		this.primitive = primitive;
		this.size = size;
	}
}
