package com.mawen.nfsdb.journal.exceptions;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class JournalConfigurationException extends JournalException {

	public JournalConfigurationException(String message) {
		super(message);
	}

	public JournalConfigurationException(String message, Object... args) {
		super(message, args);
	}

	public JournalConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}

	public JournalConfigurationException(String message, Throwable cause, Object... args) {
		super(message, cause, args);
	}
}
