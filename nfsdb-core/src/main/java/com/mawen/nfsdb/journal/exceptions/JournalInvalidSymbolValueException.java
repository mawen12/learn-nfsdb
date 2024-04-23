package com.mawen.nfsdb.journal.exceptions;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class JournalInvalidSymbolValueException extends JournalRuntimeException{
	public JournalInvalidSymbolValueException(String message, Object... args) {
		super(message, args);
	}
}
