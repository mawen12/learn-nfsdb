package com.mawen.nfsdb.journal.printer.appender;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class FileAppender implements Appender{

	private FileOutputStream fos;
	private OutputStreamAppender delegate;

	public FileAppender(File file) throws FileNotFoundException {
		this.fos = new FileOutputStream(file);
		this.delegate = new OutputStreamAppender(fos);
	}

	@Override
	public void append(StringBuilder stringBuilder) throws IOException {
		delegate.append(stringBuilder);
	}

	@Override
	public void close() throws IOException {
		delegate.close();
		fos.close();
	}
}
