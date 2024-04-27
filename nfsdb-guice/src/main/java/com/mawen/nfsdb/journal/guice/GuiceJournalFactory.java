package com.mawen.nfsdb.journal.guice;

import com.mawen.nfsdb.journal.exceptions.JournalConfigurationException;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.factory.JournalFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/27
 */
public class GuiceJournalFactory extends JournalFactory {

	public GuiceJournalFactory(JournalConfiguration configuration) {
		super(configuration);
	}
}
