package com.mawen.nfsdb.journal.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;

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

	public static void writeStringToFile(File file, String s) throws JournalException {
		try {
			try (FileOutputStream fos = new FileOutputStream(file)) {
				fos.write(s.getBytes(UTF_8));
			}
		}
		catch (IOException e) {
			throw new JournalException("Cannot write to %s", e, file.getAbsolutePath());
		}
	}

	public static String readStringFromFile(File file) throws JournalException {
		try {
			try (FileInputStream fis = new FileInputStream(file)) {
				byte[] buffer = new byte[(int) fis.getChannel().size()];
				byte b;
				int index = 0;
				while ((b = (byte) fis.read()) != -1) {
					buffer[index++] = b;
				}
				return new String(buffer, UTF_8);
			}
		}
		catch (IOException e) {
			throw new JournalException("Cannot read from %s", e, file.getAbsolutePath());
		}
	}

	public static File makeTempDir() {
		File result;
		try {
			result = File.createTempFile("journal", "");
			deleteOrException(result);
			mkDirsOrException(result);
		}
		catch (Exception e) {
			throw new JournalRuntimeException("Exception when creating temp dir", e);
		}
		return result;
	}

	public static void mkDirsOrException(File dir) {
		if (!dir.mkdirs()) {
			throw new JournalRuntimeException("Cannot create temp directory %s", dir);
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
