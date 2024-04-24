package com.mawen.nfsdb.journal.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.factory.JournalMetadata;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public final class CheckSum {

	private static final ThreadLocal<MessageDigest> localMd = new ThreadLocal<>();
	private static final ThreadLocal<ByteBuffer> localBuf = new ThreadLocal<>();

	public static byte[] getCheckSum(JournalMetadata<?> metadata) throws JournalRuntimeException {
		try {
			MessageDigest md = localMd.get();
			if (md == null) {
				md = MessageDigest.getInstance("SHA");
				localMd.set(md);
			}

			ByteBuffer buf = localBuf.get();
			if (buf == null) {
				buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
				localBuf.set(buf);
			}
			buf.clear();
			String type = metadata.getModelClass().getName();

			// model class
			flushBuf(md, buf, type.length() * 2).put(type.getBytes(Files.UTF_8));
			flushBuf(md, buf, metadata.getPartitionType().name().length() * 2).put(metadata.getPartitionType().name().getBytes(Files.UTF_8));
			for (int i = 0; i < metadata.getColumnCount(); i++) {
				JournalMetadata.ColumnMetadata m = metadata.getColumnMetadata(i);
				flushBuf(md, buf, m.name.length() * 2).put(m.name.getBytes(Files.UTF_8));
				flushBuf(md, buf, 4).putInt(m.size);
				flushBuf(md, buf, 4).putInt(m.distinctCountHint);
				flushBuf(md, buf, 1).put((byte) (m.indexed ? 1 : 0));
				if (m.sameAs != null) {
					flushBuf(md, buf, m.sameAs.length() * 2).put(m.sameAs.getBytes(Files.UTF_8));
				}
			}
			buf.flip();
			md.update(buf);
			return md.digest();
		}
		catch (NoSuchAlgorithmException e) {
			throw new JournalRuntimeException("Cannot create MD5 digest.", e);
		}
	}

	private static ByteBuffer flushBuf(MessageDigest md, ByteBuffer buf, int len) {
		if (buf.remaining() < len) {
			buf.flip();
			md.update(buf);
			buf.clear();
		}
		return buf;
	}

	private CheckSum() {}
}
