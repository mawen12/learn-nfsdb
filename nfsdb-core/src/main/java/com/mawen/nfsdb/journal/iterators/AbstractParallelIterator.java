package com.mawen.nfsdb.journal.iterators;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.mawen.nfsdb.journal.concurrent.NamedDaemonThreadFactory;
import com.mawen.nfsdb.journal.exceptions.JournalRuntimeException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/24
 */
public abstract class AbstractParallelIterator<T> implements EventFactory<AbstractParallelIterator.Holder<T>>, ParallelIterator<T> {
	RingBuffer<Holder<T>> buffer;
	SequenceBarrier barrier;
	private final int bufferSize;
	private final ExecutorService service;
	private Sequence sequence;
	private long nextSequence;
	private long availableSequence;
	private boolean started = false;

	AbstractParallelIterator(int bufferSize) {
		this.bufferSize = bufferSize;
		this.service = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("jj-iterator", false));
	}

	@Override
	public Holder<T> newInstance() {
		Holder<T> h = new Holder<>();
		h.object = getJournal().newObject();
		h.hasNext = true;
		return h;
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {

		if (!started) {
			start();
			started = true;
		}

		if (availableSequence >= nextSequence) {
			return buffer.get(nextSequence).hasNext;
		}

		try {
			availableSequence = barrier.waitFor(nextSequence);
			return availableSequence >= nextSequence && buffer.get(nextSequence).hasNext;
		}
		catch (AlertException | InterruptedException | TimeoutException e) {
			throw new JournalRuntimeException(e);
		}
	}

	@Override
	public T next() {
		hasNext();
		T result = null;

		if (availableSequence >= nextSequence) {
			result = buffer.get(nextSequence).object;
			nextSequence++;
		}
		sequence.set(nextSequence - 2);

		return result;
	}

	@Override
	public void close() {
		service.shutdown();
		barrier.alert();
	}

	void start() {
		this.buffer = RingBuffer.createSingleProducer(this, bufferSize, new BlockingWaitStrategy());
		this.barrier = buffer.newBarrier();
		this.sequence = new Sequence(barrier.getCursor());
		this.nextSequence = sequence.get() + 1L;
		this.availableSequence = -1L;
		this.buffer.addGatingSequences(sequence);
		service.submit(getRunnable());
	}

	protected abstract Runnable getRunnable();

	protected static final class Holder<T> {
		T object;
		boolean hasNext;
	}
}
