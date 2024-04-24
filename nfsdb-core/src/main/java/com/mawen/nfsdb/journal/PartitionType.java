package com.mawen.nfsdb.journal;

/**
 * Setting partition type on JournalKey to override default settings in nfsdb.xml.
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public enum PartitionType {

	DAY,

	MONTH,

	YEAR,

	/**
	 * Data is not partitioned at all,
	 * all data is stored in a single directory
	 */
	NONE,

	/**
	 * Setting partition type to DEFAULT will use whatever partition type is specified is nfsdb.xml
	 */
	DEFAULT
}
