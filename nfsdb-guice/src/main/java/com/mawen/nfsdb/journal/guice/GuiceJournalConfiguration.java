package com.mawen.nfsdb.journal.guice;

import java.io.File;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/27
 */
public class GuiceJournalConfiguration extends JournalConfiguration {

	@Inject
	public GuiceJournalConfiguration(ConfigHolder holder) {
		super(holder.config, holder.base);
	}

	static class ConfigHolder {

		@Inject(optional = true)
		@Named("nfsdb.config")
		private final String config = "/nfsdb.xml";

		@Inject
		@Named("nfsdb.base")
		private File base;
	}
}
