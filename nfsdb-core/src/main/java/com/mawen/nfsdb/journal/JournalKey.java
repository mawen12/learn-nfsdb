package com.mawen.nfsdb.journal;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.utils.ByteBuffers;
import com.mawen.nfsdb.journal.utils.Files;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public class JournalKey<T> {

	private final String clazz;
	private String location;
	private PartitionType partitionType = PartitionType.DEFAULT;
	private int recordHint = JournalConfiguration.NULL_RECORD_HINT;
	private boolean ordered = true;

	public JournalKey(Class<T> clazz) {
		this.clazz = clazz.getName();
	}

	public JournalKey(Class<T> clazz, int recordHint) {
		this.clazz = clazz.getName();
		this.recordHint = recordHint;
	}

	public JournalKey(Class<T> clazz, String location) {
		this.clazz = clazz.getName();
		this.location = location;
	}

	public JournalKey(String clazz, String location) {
		this.clazz = clazz;
		this.location = location;
	}

	public JournalKey(String clazz, String location, PartitionType partitionType) {
		this.clazz = clazz;
		this.location = location;
		this.partitionType = partitionType;
	}

	public JournalKey(Class<T> clazz, String location, PartitionType partitionType, int recordHint) {
		this.clazz = clazz.getName();
		this.location = location;
		this.partitionType = partitionType;
		this.recordHint = recordHint;
	}

	public JournalKey(Class<T> clazz, String location, PartitionType partitionType, int recordHint, boolean ordered) {
		this.clazz = clazz.getName();
		this.location = location;
		this.partitionType = partitionType;
		this.recordHint = recordHint;
		this.ordered = ordered;
	}

	public JournalKey(Class<T> clazz, String location, PartitionType partitionType, boolean ordered) {
		this.clazz = clazz.getName();
		this.location = location;
		this.partitionType = partitionType;
		this.ordered = ordered;
	}

	public static JournalKey<Object> fromBuffer(ByteBuffer buffer) {
		// clazz
		int clazzLen = buffer.getInt();
		byte[] clazz = new byte[clazzLen];
		buffer.get(clazz);
		// location
		int locLen = buffer.getInt();
		char[] location = null;
		if (locLen > 0) {
			location = new char[locLen];
			for (int i = 0; i < locLen; i++) {
				location[i] = buffer.getChar();
			}
		}
		// partitionType
		PartitionType partitionType = PartitionType.values()[buffer.get()];
		// recordHint
		int recordHint = buffer.getInt();
		// ordered
		boolean ordered = buffer.get() == 1;

		return new JournalKey<>(new String(clazz, Files.UTF_8), location == null ? null : new String(location), partitionType, recordHint, ordered);
	}

	public String getClazz() {
		return clazz;
	}

	public String getLocation() {
		return location;
	}

	public PartitionType getPartitionType() {
		return partitionType;
	}

	public int getRecordHint() {
		return recordHint;
	}

	public boolean isOrdered() {
		return ordered;
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		JournalKey<?> that = (JournalKey<?>) object;
		return recordHint == that.recordHint
				&& ordered == that.ordered
				&& Objects.equals(clazz, that.clazz)
				&& Objects.equals(location, that.location)
				&& partitionType == that.partitionType;
	}

	@Override
	public int hashCode() {
		return Objects.hash(clazz, location, partitionType, recordHint, ordered);
	}

	@Override
	public String toString() {
		return "JournalKey{" +
				"clazz='" + clazz + '\'' +
				", location='" + location + '\'' +
				", partitionType=" + partitionType +
				", recordHint=" + recordHint +
				", ordered=" + ordered +
				'}';
	}

	//////////////////////// REPLICATION CODE //////////////////////

	public int getBufferSize() {
		return 4 + clazz.getBytes(Files.UTF_8).length + 4
				+ 2 * (location == null ? 0 : location.length())
				+ 1 + 1 + 4;
	}

	public void write(ByteBuffer buffer) {
		// clazz
		buffer.putInt(clazz.length());
		for (byte b : clazz.getBytes(Files.UTF_8)) {
			buffer.put(b);
		}
		// location
		ByteBuffers.putStringDW(buffer, location);
		// partition type
		buffer.put((byte) partitionType.ordinal());
		// recordHint
		buffer.putInt(recordHint);
		// ordered
		buffer.put((byte) (ordered ? 1 : 0));
	}

	/////////////////////////////////////////////////////////////////

	private JournalKey(String clazz, String location, PartitionType partitionType, int recordHint, boolean ordered) {
		this.clazz = clazz;
		this.location = location;
		this.partitionType = partitionType;
		this.recordHint = recordHint;
		this.ordered = ordered;
	}
}
