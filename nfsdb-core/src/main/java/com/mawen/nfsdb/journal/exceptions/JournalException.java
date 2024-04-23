package com.mawen.nfsdb.journal.exceptions;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class JournalException extends Exception {

	public JournalException() {}

	public JournalException(String message, Object... args) {
		super(args.length == 0 ? message : String.format(message, args));
	}

	public JournalException(String message, Throwable cause, Object... args) {
		super(args.length == 0 ? message : String.format(message, args), cause);
	}

	public JournalException(Throwable cause) {
		super(cause);
	}
}
