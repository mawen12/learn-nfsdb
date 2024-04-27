package com.mawen.nfsdb.journal.guice;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.factory.JournalPool;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/27
 */
public class GuiceJournalPool extends JournalPool {

	@Inject
	public GuiceJournalPool(JournalConfiguration configuration, SizeHolder holder) {
		super(configuration, holder.maxSize);
	}

	static class SizeHolder {

		@Inject(optional = true)
		@Named("nsfdb.journal.pool.size")
		private final int maxSize = 10;
	}
}
