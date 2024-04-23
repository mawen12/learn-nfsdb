package com.mawen.nfsdb.journal.column;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import com.mawen.nfsdb.journal.JournalMode;
import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalNoSuchFileException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;
import com.mawen.nfsdb.journal.logging.Logger;
import com.mawen.nfsdb.journal.utils.ByteBuffers;
import com.mawen.nfsdb.journal.utils.Files;
import com.mawen.nfsdb.journal.utils.Lists;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class MappedFileImpl implements MappedFile {

	private static final Logger LOGGER = Logger.getLogger(MappedFileImpl.class);
	private final File file;
	private final JournalMode mode;
	private final int bitHint;
	// reserve first 8 bytes in the file for storing pointer to logical end of file
	// so the actual data begins from "dataOffset"
	private final int dataOffset = 8;
	private FileChannel channel;
	private ByteBuffer offsetBuffer;
	private List<ByteBufferWrapper> buffers;
	private List<ByteBufferWrapper> stitches;

	public MappedFileImpl(File file, int bitHint, JournalMode mode) throws JournalException {
		this.file = file;
		this.bitHint = bitHint;
		this.mode = mode;
		open();
		this.buffers = new ArrayList<>((int) (size() >>> bitHint) + 1);
		this.stitches = new ArrayList<>(buffers.size());
	}

	/////////////////////////////////////////////////////////////////

	@Override
	public ByteBufferWrapper getBuffer(long offset, int size) {

		int bufferSize = 1 << bitHint;
		int bufferIndex = (int) (offset >>> bitHint);
		long bufferOffset = bufferIndex * bufferSize;
		int bufferPos = (int) (offset - bufferOffset);

		Lists.advance(buffers, bufferIndex);

		ByteBufferWrapper buffer = buffers.get(bufferIndex);

		if (buffer != null && buffer.getByteBuffer().limit() < bufferPos) {
			buffer.release();
			buffer = null;
		}

		if (buffer == null) {
			buffer = new ByteBufferWrapper(bufferOffset, mapBufferInternal(bufferOffset, bufferSize));
			assert bufferSize > 0;
			buffers.set(bufferIndex, buffer);
		}

		buffer.getByteBuffer().position(bufferPos);

		// if the desired size is larger than remaining buffer we need to crate
		// a stitch buffer, which would accommodate the size
		if (buffer.getByteBuffer().remaining() < size) {

			Lists.advance(stitches, bufferIndex);

			buffer = stitches.get(bufferIndex);
			long stitchOffset = bufferOffset + bufferPos;
			if (buffer != null) {
				// if we already have a stitch for this buffer
				// it could be too small for the size
				// if that's the case - discard the existing stitch and create a larger one.
				if (buffer.getOffset() != stitchOffset || buffer.getByteBuffer().limit() < size) {
					buffer.release();
					buffer = null;
				}
				else {
					buffer.getByteBuffer().rewind();
				}
			}

			if (buffer == null) {
				buffer = new ByteBufferWrapper(stitchOffset, mapBufferInternal(stitchOffset, size));
				stitches.set(bufferIndex, buffer);
			}
		}
		return buffer;
	}

	public void delete() {
		close();
		Files.delete(file);
	}

	@Override
	public void close() {
		try {
			releaseBuffers();
			channel.close();
		}
		catch (IOException e) {
			throw new JournalRuntimeException("Cannot close file", e);
		}
		offsetBuffer = ByteBuffers.release(offsetBuffer);
	}

	@Override
	public String toString() {
		return this.getClass().getName() + "[file=" + file + ", appendOffset=" + getAppendOffset() + "]";
	}

	@Override
	public long getAppendOffset() {
		if (offsetBuffer != null) {
			offsetBuffer.position(0);
			return offsetBuffer.getLong();
		}
		return -1L;
	}

	@Override
	public void setAppendOffset(long offset) {
		offsetBuffer.position(0);
		offsetBuffer.putLong(offset);
	}

	@Override
	public void compact() throws JournalException {
		close();
		openInternal("rw");
		try {
			try {
				long newSize = getAppendOffset() + dataOffset;
				offsetBuffer = ByteBuffers.release(offsetBuffer);
				LOGGER.debug("Compacting %s to %d bytes", this, newSize);
				channel.truncate(newSize).close();
			}
			catch (IOException e) {
				throw new JournalException("Could not compact %s to %d bytes", e, getFullFileName(), getAppendOffset());
			}
			finally {
				close();
			}
		}
		finally {
			open();
		}
	}

	public String getFullFileName() {
		return this.file.getAbsolutePath();
	}

	/////////////////////////////////////////////////////////////////

	private long size() throws JournalException {
		try {
			return channel.size();
		}
		catch (IOException e) {
			throw new JournalException("Could not get channel size", e);
		}
	}

	private void open() throws JournalException {
		String m;
		switch (mode) {
			case READ:
				m = "r";
				break;
			case APPEND:
			default:
				m = "rw";
		}
		openInternal(m);
	}

	private void openInternal(String mode) throws JournalException {
		if (!file.getParentFile().exists()) {
			if (!file.getParentFile().mkdir()) {
				throw new JournalException("Could not create directories: %s", file.getParentFile().getAbsolutePath());
			}
		}

		try {
			FileChannel offsetChannel = new RandomAccessFile(file, mode).getChannel();
			if ("r".equals(mode)) {
				this.offsetBuffer = offsetChannel.map(FileChannel.MapMode.READ_ONLY, 0, Math.min(offsetChannel.size(), 8));
			}
			else {
				this.offsetBuffer = offsetChannel.map(FileChannel.MapMode.READ_WRITE, 0, 8);
			}
		}
		catch (FileNotFoundException e) {
			throw new JournalNoSuchFileException(e);
		}
		catch (IOException e) {
			throw new JournalException(e);
		}
	}

	private ByteBuffer mapBufferInternal(long offset, int size) {
		long actualOffset = offset + dataOffset;

		try {
			switch (mode) {
				case READ:
					long sz;
					if (actualOffset + size > channel.size()) {
						sz = channel.size() - actualOffset;
					}
					else {
						sz = size;
					}
					assert sz > 0;
					return channel.map(FileChannel.MapMode.READ_ONLY, actualOffset, sz).order(ByteOrder.LITTLE_ENDIAN);
				case APPEND:
				default:
					return channel.map(FileChannel.MapMode.READ_WRITE, actualOffset, size).order(ByteOrder.LITTLE_ENDIAN);
			}
		}
		catch (IOException e) {
			throw new JournalRuntimeException("Failed to memory map: %s", e, file.getAbsolutePath());
		}
	}

	private void releaseBuffers() {
		for (int i = 0, bufferSize = buffers.size(); i < bufferSize; i++) {
			ByteBufferWrapper b = buffers.get(i);
			if (b != null) {
				b.release();
			}
		}
		for (int i = 0, stitchesSize = stitches.size(); i < stitchesSize; i++) {
			ByteBufferWrapper b = stitches.get(i);
			if (b != null) {
				b.release();
			}
		}
		buffers.clear();
		stitches.clear();
	}
}
