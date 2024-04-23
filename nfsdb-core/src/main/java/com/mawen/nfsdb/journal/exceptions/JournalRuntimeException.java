package com.mawen.nfsdb.journal.exceptions;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class JournalRuntimeException extends RuntimeException {

	public JournalRuntimeException(String message, Object... args) {
		super(args.length == 0 ? message : String.format(message, args));
	}

	public JournalRuntimeException(String message, Throwable cause, Object... args) {
		super(args.length == 0 ? message : String.format(message, args), cause);
	}

	public JournalRuntimeException(Throwable cause) {
		super(cause);
	}
}
