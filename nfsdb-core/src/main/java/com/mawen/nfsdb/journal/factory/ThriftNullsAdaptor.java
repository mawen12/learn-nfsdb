package com.mawen.nfsdb.journal.factory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.BitSet;

import com.mawen.nfsdb.journal.collections.IntArrayList;
import com.mawen.nfsdb.journal.column.ColumnType;
import com.mawen.nfsdb.journal.exceptions.JournalConfigurationException;
import com.mawen.nfsdb.journal.exceptions.JournalUnSupportedTypeException;
import com.mawen.nfsdb.journal.utils.Unsafe;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class ThriftNullsAdaptor<T> implements NullsAdaptor<T> {

	private final IntArrayList fieldMapping = new IntArrayList();
	private BitFieldType bitFieldType;
	private long bitFieldOffset;

	public ThriftNullsAdaptor(Class<T> modelClass) throws JournalConfigurationException {

		int fieldCount = 0;
		Field[] classFields = modelClass.getDeclaredFields();
		for (Field f : classFields) {

			if (Modifier.isStatic(f.getModifiers())) {
				continue;
			}

			if ("__isset_bitfield".equals(f.getName())) {
				Class<?> type = f.getType();
				if (type == byte.class) {
					bitFieldType = BitFieldType.BYTE;
				}
				else if (type == short.class) {
					bitFieldType = BitFieldType.SHORT;
				}
				else if (type == int.class) {
					bitFieldType = BitFieldType.INT;
				}
				else if (type == long.class) {
					bitFieldType = BitFieldType.LONG;
				}
				else if (type == BitSet.class) {
					bitFieldType = BitFieldType.BIT_SET;
				}
				else {
					throw new JournalConfigurationException("Unsupported bitfield type: %s. Unsupported Thrift version?", f.getName());
				}
				bitFieldOffset = Unsafe.getUnsafe().objectFieldOffset(f);
				continue;
			}

			if ("__isset_bit_vector".equals(f.getName())) {
				bitFieldType = BitFieldType.BIT_SET;
				bitFieldOffset = Unsafe.getUnsafe().objectFieldOffset(f);
				continue;
			}

			Class<?> type = f.getType();
			for (ColumnType t : ColumnType.values()) {
				if (t.matches(type)) {
					if (t.primitive()) {
						fieldMapping.add(fieldCount);
					}
					fieldCount++;
					break;
				}
			}
		}
	}

	@Override
	public void setNulls(T obj, BitSet src) {
		switch (bitFieldType) {
			case BIT_SET:
				setNullsBitSet(obj, src);
				break;
			default:
				setNullsBitField(obj, src);
		}
	}

	@Override
	public void getNulls(T obj, BitSet dst) {
		switch (bitFieldType) {
			case BIT_SET:
				getNullsBitSet(obj, dst);
				break;
			default:
				getNullsBitField(obj, dst);
		}
	}

	@Override
	public void clear(T obj) {
		switch (bitFieldType) {
			case BIT_SET:
				((BitSet) Unsafe.getUnsafe().getObject(obj, bitFieldOffset)).clear();
				break;
			default:
				switch (bitFieldType) {
					case BYTE:
						Unsafe.getUnsafe().putByte(obj, bitFieldOffset, (byte) 0);
						break;
					case SHORT:
						Unsafe.getUnsafe().putShort(obj, bitFieldOffset, (short) 0);
						break;
					case INT:
						Unsafe.getUnsafe().putInt(obj, bitFieldOffset, 0);
						break;
					case LONG:
						Unsafe.getUnsafe().putLong(obj, bitFieldOffset, 0L);
						break;
					default:
						throw new JournalUnSupportedTypeException(bitFieldType);
				}
		}
	}

	private void setNullsBitField(T obj, BitSet src) {

		long bitField;

		switch (bitFieldType) {
			case BYTE:
				bitField = Unsafe.getUnsafe().getByte(obj, bitFieldOffset);
				break;
			case SHORT:
				bitField = Unsafe.getUnsafe().getShort(obj, bitFieldOffset);
				break;
			case INT:
				bitField = Unsafe.getUnsafe().getInt(obj, bitFieldOffset);
				break;
			case LONG:
				bitField = Unsafe.getUnsafe().getLong(obj, bitFieldOffset);
				break;
			default:
				throw new JournalUnSupportedTypeException(bitFieldType);
		}

		for (int i = 0, sz = fieldMapping.size(); i < sz; i++) {
			bitField = src.get(fieldMapping.getQuick(i)) ? bitField & ~(1 << i) : bitField | (1 << i);
		}

		switch (bitFieldType) {
			case BYTE:
				Unsafe.getUnsafe().putByte(obj, bitFieldOffset, (byte) bitField);
				break;
			case SHORT:
				Unsafe.getUnsafe().putShort(obj, bitFieldOffset, (short) bitField);
				break;
			case INT:
				Unsafe.getUnsafe().putInt(obj, bitFieldOffset, 0);
				break;
			case LONG:
				Unsafe.getUnsafe().putLong(obj, bitFieldOffset, 0L);
				break;
			default:
				throw new JournalUnSupportedTypeException(bitFieldType);
		}
	}

	private void setNullsBitSet(T obj, BitSet src) {
		BitSet bitField = (BitSet) Unsafe.getUnsafe().getObject(obj, bitFieldOffset);

		for (int i = 0, sz = fieldMapping.size(); i < sz; i++) {
			bitField.set(i, !src.get(fieldMapping.getQuick(i)));
		}

		Unsafe.getUnsafe().putObject(obj, bitFieldOffset, bitField);
	}

	public void getNullsBitField(T obj, BitSet dst) {

		long bitField;

		switch (bitFieldType) {
			case BYTE:
				bitField = Unsafe.getUnsafe().getByte(obj, bitFieldOffset);
				break;
			case SHORT:
				bitField = Unsafe.getUnsafe().getShort(obj, bitFieldOffset);
				break;
			case INT:
				bitField = Unsafe.getUnsafe().getInt(obj, bitFieldOffset);
				break;
			case LONG:
				bitField = Unsafe.getUnsafe().getLong(obj, bitFieldOffset);
				break;
			default:
				throw new JournalUnSupportedTypeException(bitFieldType);
		}

		for (int i = 0, sz = fieldMapping.size(); i < sz; i++) {
			if ((bitField & (1 << i)) == 0) {
				dst.set(fieldMapping.getQuick(i));
			}
			else {
				dst.clear(fieldMapping.getQuick(i));
			}
		}
	}

	private void getNullsBitSet(T obj, BitSet dst) {
		BitSet bitField = (BitSet) Unsafe.getUnsafe().getObject(obj, bitFieldOffset);

		for (int i = 0, sz = fieldMapping.size(); i < sz; i++) {
			dst.set(fieldMapping.getQuick(i), !bitField.get(i));
		}
	}

	private enum BitFieldType {
		BYTE, SHORT, INT, LONG, FLOAT, BIT_SET
	}
}
