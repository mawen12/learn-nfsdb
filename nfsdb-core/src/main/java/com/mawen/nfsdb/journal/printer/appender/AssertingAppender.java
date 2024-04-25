package com.mawen.nfsdb.journal.printer.appender;

import java.io.IOException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class AssertingAppender implements Appender {

	private final String[] expected;
	private int index;

	public AssertingAppender(String expected) {
		this.expected = expected.split("\n");
	}

	@Override
	public void append(StringBuilder stringBuilder) throws IOException {
		if (index < expected.length) {
			String s = stringBuilder.toString();
			if (!expected[index].equals(s)) {
				throw new AssertionError(("\n\n>>>> Expected [ " + (index + 1) + " ]>>>>\n") + expected[index]
				                         + "\n<<<< Actual <<<<\n" + s + "\n");
			}
		}
		else {
			throw new AssertionError("!!! Expected " + expected.length + " lines, actual " + index);
		}
		index++;
	}

	@Override
	public void close() throws IOException {
		if (index < expected.length) {
			throw new AssertionError("!!! Too few rows. Expected " + expected.length + ", actual " + index);
		}
	}
}
