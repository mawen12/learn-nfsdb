package com.mawen.nfsdb.journal.factory;

import java.util.HashMap;

import com.mawen.nfsdb.journal.logging.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class JournalConfiguration {
	private static final Logger LOGGER = Logger.getLogger(JournalConfiguration.class);

	public static final String TEMP_DIRECTORY_PREFIX = "temp";
	public static final String DEFAULT_CONFIG_FILE = "/mawen_nfsdb.xml";
	public static final String JOURNAL_META_FILE = "_meta";

	/////////////////////////////////////////////////////////////////

	public static final int PIPE_BIT_HINT = 16;

	/////////////////////////////////////////////////////////////////

	public static final int VARCHAR_INDEX_COLUMN_WIDTH = 8;
	public static final int VARCHAR_SHORT_HEADER_LENGTH = 1;
	public static final int VARCHAR_MEDIUM_HEADER_LENGTH = 2;
	public static final int VARCHAR_LARGE_HEADER_LENGTH = 3;

	/////////////////////////////////////////////////////////////////

	public static final int DEFAULT_RECORD_HINT = 1_000_000;
	public static final int DEFAULT_STRING_AVG_SIZE = 12;
	public static final int DEFAULT_STRING_MAX_SIZE = 255;
	public static final int DEFAULT_SYMBOL_MAX_SIZE = 128;
	public static final int DEFAULT_DISTINCT_COUNT_HINT = 255;
	public static final int NULL_RECORD_HINT = 0;
	public static final int OPEN_PARTITION_TTL = 60;
	public static final int DEFAULT_LOG_HOURS = 0;

	/////////////////////////////////////////////////////////////////

	private final Map<String, JournalMetadata> metadataMap = new HashMap<>();

	/////////////////////////////////////////////////////////////////
}
