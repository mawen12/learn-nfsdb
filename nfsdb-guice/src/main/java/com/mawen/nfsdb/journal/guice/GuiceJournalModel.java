package com.mawen.nfsdb.journal.guice;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.factory.JournalFactory;
import com.mawen.nfsdb.journal.factory.JournalPool;
import com.mawen.nfsdb.journal.factory.JournalReaderFactory;
import com.mawen.nfsdb.journal.factory.JournalWriterFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/27
 */
public class GuiceJournalModel extends AbstractModule {

	public static final String GLOBAL_POOL = "nfsdb-global-pool";
	// overriding factory
	private final JournalConfiguration configuration;

	public GuiceJournalModel() {
		this.configuration = null;
	}

	public GuiceJournalModel(JournalConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	protected void configure() {
		if (configuration != null) {
			bind(JournalConfiguration.class).toInstance(configuration);
		}
		else {
			bind(JournalConfiguration.class).to(GuiceJournalConfiguration.class).asEagerSingleton();
		}

		bind(JournalReaderFactory.class).to(GuiceJournalFactory.class).asEagerSingleton();
		bind(JournalWriterFactory.class).to(GuiceJournalFactory.class).asEagerSingleton();
		bind(JournalFactory.class).to(GuiceJournalFactory.class).asEagerSingleton();
		bind(JournalPool.class).annotatedWith(Names.named(GLOBAL_POOL)).to(GuiceJournalPool.class).asEagerSingleton();
		bind(JournalPool.class).to(GuiceJournalPool.class);
	}
}
