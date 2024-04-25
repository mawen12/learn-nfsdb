package com.mawen.nfsdb.journal.printer.appender;

import java.io.IOException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public final class StdOutAppender implements Appender{

	public static final StdOutAppender INSTANCE = new StdOutAppender();

	@Override
	public void append(StringBuilder stringBuilder) throws IOException {
		System.out.println(stringBuilder);
	}

	@Override
	public void close() throws IOException {
	}

	private StdOutAppender() {}
}
