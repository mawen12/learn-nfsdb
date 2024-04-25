package com.mawen.nfsdb.journal.iterators;

import java.util.Comparator;
import java.util.Iterator;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/25
 */
public class MergingIterator<T> implements Iterator<T>, Iterable<T> {

	private final Iterator<T> a;
	private final Iterator<T> b;
	private final Comparator<T> comparator;
	private T nextA;
	private T nextB;

	public MergingIterator(Iterator<T> a, Iterator<T> b, Comparator<T> comparator) {
		this.a = a;
		this.b = b;
		this.comparator = comparator;
	}

	@Override
	public boolean hasNext() {
		return nextA != null || a.hasNext() || nextB != null || b.hasNext();
	}

	@Override
	public T next() {
		T result;

		if (nextA == null && a.hasNext()) {
			nextA = a.next();
		}

		if (nextB == null && b.hasNext()) {
			nextB = b.next();
		}

		if (nextB == null || (nextA != null && comparator.compare(nextA, nextB) < 0)) {
			result = nextA;
			nextA = null;
		}
		else {
			result = nextB;
			nextB = null;
		}

		return result;
	}

	@Override
	public void remove() {

	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}
}
