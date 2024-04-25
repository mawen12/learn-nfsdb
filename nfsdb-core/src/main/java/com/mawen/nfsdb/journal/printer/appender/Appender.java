package com.mawen.nfsdb.journal.printer.appender;

import java.io.IOException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public interface Appender {

	void append(StringBuilder stringBuilder) throws IOException;

	void close() throws IOException;
}
