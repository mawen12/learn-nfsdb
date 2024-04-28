package com.mawen.nfsdb.journal.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.mawen.nfsdb.journal.exceptions.JournalException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link Files}
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/28
 */
public class FilesUnitTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testWriteStringToFile() throws JournalException, IOException {
		File f = temporaryFolder.newFile();
		Files.writeStringToFile(f, "TEST123");
		Assert.assertEquals("TEST123", Files.readStringFromFile(f));
	}

	@Test
	public void testDeleteDir() throws IOException {
		File f = temporaryFolder.newFolder("to_delete");
		Assert.assertTrue(new File(f, "a/b/c").mkdirs());
		Assert.assertTrue(new File(f, "d/e/f").mkdirs());
		touch(new File(f, "d/1.txt"));
		touch(new File(f, "a/b/2.txt"));
		Assert.assertTrue(Files.delete(f));
		Assert.assertFalse(f.exists());
	}

	private static void touch(File file) throws IOException {
		FileOutputStream fos = new FileOutputStream(file);
		fos.close();
	}
}
