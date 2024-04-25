package com.mawen.nfsdb.journal.locks;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.logging.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public final class LockManager {

	private static final Logger LOGGER = Logger.getLogger(LockManager.class);
	private static final Map<String, Lock> locks = new ConcurrentHashMap<>();

	public static Lock lockExclusive(File location) throws JournalException {
		String shardKey = getKey(location, true);
		String exclusiveKey = getKey(location, false);

		Lock lock = locks.get(shardKey);

		if (lock == null) {
			// we have an exclusive lock in our class loader, fail early
			lock = locks.get(exclusiveKey);
			if (lock != null) {
				return null;
			}

			lock = new Lock(location, true);
			locks.put(shardKey, lock);
		}

		lock.incrementRefCount();
		LOGGER.trace("Shared lock was successful: %s", lock);
		return lock;
	}

	public static Lock lockShared(File location) throws JournalException {
		String sharedKey = getKey(location, true);
		String exclusiveKey = getKey(location, false);

		Lock lock = locks.get(sharedKey);

		if (lock == null) {
			// we have an exclusive lock in our class loader, fail early
			lock = locks.get(exclusiveKey);
			if (lock != null) {
				return null;
			}

			lock = new Lock(location, true);
			locks.put(sharedKey, lock);
		}

		lock.incrementRefCount();
		LOGGER.trace("Shared lock was successful: %s", lock);
		return lock;
	}

	public static void release(Lock lock) {
		if (lock == null) {
			return;
		}

		String sharedKey = getKey(lock.getLocation(), true);
		String exclusiveKey = getKey(lock.getLocation(), false);

		Lock storedSharedLock = locks.get(sharedKey);
		if (storedSharedLock == lock) {
			lock.decrementRefCount();
			if (lock.getRefCount() <= 0) {
				lock.release();
				locks.remove(sharedKey);
				LOGGER.trace("Shared lock released: %s", lock);
			}
		}

		Lock storedExclusiveLock = locks.get(exclusiveKey);
		if (storedExclusiveLock == lock) {
			lock.decrementRefCount();
			if (lock.getRefCount() <= 0) {
				lock.release();
				lock.delete();
				locks.remove(exclusiveKey);
				LOGGER.trace("Exclusive lock released: %s", lock);
			}
		}
	}

	private static String getKey(File location, boolean shared) {
		return (shared ? "ShLck:" : "ExLck:") + location.getAbsolutePath();
	}

	private LockManager() {}
}
