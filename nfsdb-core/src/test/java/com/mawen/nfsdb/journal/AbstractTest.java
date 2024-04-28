package com.mawen.nfsdb.journal;

import org.junit.Rule;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/28
 */
public class AbstractTest {

	@Rule
	public final JournalTestFactory factory = new JournalTestFactory();
}
