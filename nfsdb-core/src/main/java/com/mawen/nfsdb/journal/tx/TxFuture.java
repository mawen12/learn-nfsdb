package com.mawen.nfsdb.journal.tx;

import java.util.concurrent.TimeUnit;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public interface TxFuture {

	boolean waitFor(long time, TimeUnit unit);
}
