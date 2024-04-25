package com.mawen.nfsdb.journal.factory;

import java.io.File;

import com.mawen.nfsdb.journal.JournalKey;
import com.mawen.nfsdb.journal.JournalWriter;
import com.mawen.nfsdb.journal.PartitionType;
import com.mawen.nfsdb.journal.exceptions.JournalConfigurationException;
import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class JournalFactory extends AbstractJournalReaderFactory implements JournalReaderFactory, JournalWriterFactory {

	public JournalFactory(String journalBase) throws JournalConfigurationException {
		this(new File(journalBase));
	}

	public JournalFactory(File journalBase) throws JournalConfigurationException {
		this(new JournalConfiguration(journalBase).build());
	}

	public JournalFactory(JournalConfiguration journalConfiguration) {
		super(journalConfiguration);
	}

	@Override
	public <T> JournalWriter<T> writer(Class<T> clazz) throws JournalException {
		return writer(new JournalKey<>(clazz));
	}

	@Override
	public <T> JournalWriter<T> writer(Class<T> clazz, String location) throws JournalException {
		return writer(new JournalKey<>(clazz, location));
	}

	@Override
	public <T> JournalWriter<T> writer(Class<T> clazz, String location, int recordHint) throws JournalException {
		return writer(new JournalKey<>(clazz, location, PartitionType.DEFAULT, recordHint));
	}

	public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
		return new JournalWriter<>(getConfiguration().getMetadata(key), key, getTimerCache());
	}
}
