package com.mawen.nfsdb.journal.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public final class Lists {

	public static void advance(List<?> list, int index) {
		while (list.size() <= index) {
			list.add(null);
		}
	}

	public static <E> List<E> asList(E... e) {
		ArrayList<E> result = new ArrayList<>(e.length);
		Collections.addAll(result, e);
		return result;
	}

	private Lists() {}
}
