package com.mawen.nfsdb.journal;

import java.util.ArrayList;
import java.util.List;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.factory.JournalClosingListener;
import com.mawen.nfsdb.journal.factory.JournalConfiguration;
import com.mawen.nfsdb.journal.factory.JournalFactory;
import com.mawen.nfsdb.journal.utils.Files;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/28
 */
public class JournalTestFactory extends JournalFactory implements TestRule, JournalClosingListener {

	private final List<Journal> journals = new ArrayList<>();

	public JournalTestFactory() {
		super(new JournalConfiguration(Files.makeTempDir()));
	}

	@Override
	public Statement apply(Statement base, Description desc) {

		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				Throwable throwable = null;
				try {
					getConfiguration().build();
					Files.deleteOrException(getConfiguration().getJournalBase());
					Files.deleteOrException(getConfiguration().getJournalBase());
					base.evaluate();
				}
				catch (Throwable e) {
					throwable = e;
				}
				finally {
					for (Journal journal : journals) {
						if (journal.isOpen()) {
							journal.setCloseListener(null);
							journal.close();
						}
					}
					journals.clear();
					Files.delete(getConfiguration().getJournalBase());
				}

				if (throwable != null) {
					throw throwable;
				}
			}
		};
	}

	@Override
	public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
		Journal<T> result = super.reader(key);
		journals.add(result);
		result.setCloseListener(this);
		return result;
	}

	@Override
	public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
		JournalWriter<T> writer = super.writer(key);
		journals.add(writer);
		writer.setCloseListener(this);
		return writer;
	}

	@Override
	public boolean closing(Journal journal) {
		journals.remove(journal);
		return true;
	}
}
