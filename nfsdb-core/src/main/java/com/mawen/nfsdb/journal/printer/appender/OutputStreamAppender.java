package com.mawen.nfsdb.journal.printer.appender;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class OutputStreamAppender implements Appender{

	private final PrintWriter w;

	public OutputStreamAppender(OutputStream out) {
		this.w = new PrintWriter(out);
	}

	@Override
	public void append(StringBuilder stringBuilder) throws IOException {
		w.println(stringBuilder.toString());
	}

	@Override
	public void close() throws IOException {
		w.flush();
	}
}
