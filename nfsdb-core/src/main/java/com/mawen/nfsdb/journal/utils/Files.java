package com.mawen.nfsdb.journal.utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import com.mawen.nfsdb.journal.exceptions.JournalException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public abstract class Files {

	public static final Charset UTF_8;

	static {
		UTF_8 = Charset.forName("UTF-8");
	}

	public static boolean delete(File file) {
		try {
			deleteOrException(file);
			return true;
		}
		catch (JournalException e) {
			return false;
		}
	}

	public static void deleteOrException(File file) throws JournalException {
		if (!file.exists()) {
			return;
		}
		deleteDirContentsOrException(file);
		if (!file.delete()) {
			throw new JournalException("Cannot to delete file %s", file);
		}
	}

	public static void deleteDirContentsOrException(File file) throws JournalException {
		if (!file.exists()) {
			return;
		}

		try {
			if (notSymLink(file)) {
				File[] files = file.listFiles();
				if (files != null) {
					for (File f : files) {
						deleteOrException(f);
					}
				}
			}
		}
		catch (IOException e) {
			throw new JournalException("Cannot delete dir contents: %s", file, e);
		}
	}

	/////////////////////////////////////////////////////////////////

	private static boolean notSymLink(File file) throws IOException {
		if (file == null) {
			throw new NullPointerException("File must not be null");
		}
		if (File.separatorChar == '\\') {
			return true;
		}

		File fileInCanonicalDir;
		if (file.getParent() == null) {
			fileInCanonicalDir = file;
		}
		else {
			File canonicalDir = file.getParentFile().getCanonicalFile();
			fileInCanonicalDir = new File(canonicalDir, file.getName());
		}

		return fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
	}

	/////////////////////////////////////////////////////////////////

	private Files() {}
}
