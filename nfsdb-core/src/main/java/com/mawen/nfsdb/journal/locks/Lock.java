package com.mawen.nfsdb.journal.locks;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.atomic.AtomicInteger;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public final class Lock {

	private final AtomicInteger refCount = new AtomicInteger(0);
	private RandomAccessFile file;
	private FileLock lock;
	private File lockName;
	private File location;

	@Override
	public String toString() {
		return "Lock{" +
				"lockName=" + lockName +
				", isShared=" + (lock == null ? "NULL" : lock.isShared()) +
				", isValid=" + (lock == null ? "NULL" : lock.isValid()) +
				", refCount=" + refCount.get() +
				'}';
	}

	public synchronized boolean isValid() {
		return lock != null && lock.isValid();
	}

	synchronized void release() {
		try {
			if (isValid()) {
				lock.release();
				lock = null;
			}

			if (file != null) {
				file.close();
				file = null;
			}
		}
		catch (IOException e) {
			throw new JournalRuntimeException(e);
		}
	}

	synchronized void delete() {
		if (!lockName.delete()) {
			throw new JournalRuntimeException("Could not delete lock: %s", lockName);
		}
	}

	int getRefCount() {
		return refCount.get();
	}

	void incrementRefCount() {
		refCount.incrementAndGet();
	}

	void decrementRefCount() {
		refCount.decrementAndGet();
	}

	File getLocation() {
		return location;
	}

	Lock(File location, boolean shared) throws JournalException {
		if (!location.exists()) {
			if (!location.mkdirs()) {
				throw new JournalException("Could not create directory: %s", location);
			}
		}

		try {
			this.location = location;
			this.lockName = new File(location + ".lock");
			this.file = new RandomAccessFile(lockName, "rw");
			int i = 0;
			while (true) {
				try {
					this.lock = file.getChannel().tryLock(i, 1, shared);
					break;
				}
				catch (OverlappingFileLockException e) {
					if (shared) {
						i++;
					}
					else {
						this.lock = null;
						break;
					}
				}
			}
		}
		catch (IOException e) {
			throw new JournalException(e);
		}
	}
}
