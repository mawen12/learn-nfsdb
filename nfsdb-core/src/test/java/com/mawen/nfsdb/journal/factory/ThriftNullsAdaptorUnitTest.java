package com.mawen.nfsdb.journal.factory;

import java.util.BitSet;

import com.mawen.nfsdb.journal.exceptions.JournalConfigurationException;
import org.apache.thrift.partial.ThriftField;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ThriftNullsAdaptor}
 */
public class ThriftNullsAdaptorUnitTest {

	@Test
	public void testByteBitField() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<ByteFieldSample> adaptor = new ThriftNullsAdaptor<>(ByteFieldSample.class);
		BitSet source = new BitSet();
		source.set(0);
		source.set(2);
		source.set(3);

		ByteFieldSample sample = new ByteFieldSample();
		adaptor.setNulls(sample, source);

		// when && then
		Assert.assertEquals("10001", Integer.toBinaryString(sample.__isset_bitfield));
	}

	@Test
	public void testShortBitField() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<ShortFieldSample> adaptor = new ThriftNullsAdaptor<>(ShortFieldSample.class);
		BitSet source = new BitSet();
		source.set(0);
		source.set(2);
		source.set(3);

		ShortFieldSample sample = new ShortFieldSample();
		adaptor.setNulls(sample, source);

		// when
		Assert.assertEquals("1", Integer.toBinaryString(sample.__isset_bitfield));
	}

	@Test
	public void testIntBitField() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<IntFieldSample> adaptor = new ThriftNullsAdaptor<>(IntFieldSample.class);
		BitSet source = new BitSet();
		source.set(0);
		source.set(2);
		source.set(3);

		IntFieldSample sample = new IntFieldSample();
		adaptor.setNulls(sample, source);

		// when & then
		Assert.assertEquals("1", Integer.toBinaryString(sample.__isset_bitfield));
	}

	@Test
	public void testLongBitField() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<LongFieldSample> adaptor = new ThriftNullsAdaptor<>(LongFieldSample.class);
		BitSet source = new BitSet();
		source.set(0);
		source.set(2);
		source.set(3);

		LongFieldSample sample = new LongFieldSample();
		adaptor.setNulls(sample, source);

		// when & then
		Assert.assertEquals("10001", Integer.toBinaryString(sample.__isset_bitfield));
	}

	@Test
	public void testBitSet() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<BitSetSample> adaptor = new ThriftNullsAdaptor<>(BitSetSample.class);
		BitSet source = new BitSet();
		source.set(0);
		source.set(2);
		source.set(3);

		BitSetSample sample = new BitSetSample();
		adaptor.setNulls(sample, source);

		// when & then
		Assert.assertEquals("{0}", sample.__isset_bit_vector.toString());
	}

	@Test
	public void testGetByteBitField() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<ByteFieldSample> adaptor = new ThriftNullsAdaptor<>(ByteFieldSample.class);
		BitSet dst = new BitSet();

		ByteFieldSample sample = new ByteFieldSample();
		sample.__isset_bitfield = 2;
		adaptor.getNulls(sample, dst);

		// when & then
		Assert.assertEquals("{1}", dst.toString());
	}

	@Test
	public void testGetShortBitField() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<ShortFieldSample> adaptor = new ThriftNullsAdaptor<>(ShortFieldSample.class);
		BitSet dst = new BitSet();

		ShortFieldSample sample = new ShortFieldSample();
		sample.__isset_bitfield = 2;
		adaptor.getNulls(sample, dst);

		// when & then
		Assert.assertEquals("{1}", dst.toString());
	}

	@Test
	public void testGetIntBitField() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<IntFieldSample> adaptor = new ThriftNullsAdaptor<>(IntFieldSample.class);
		BitSet dst = new BitSet();

		IntFieldSample sample = new IntFieldSample();
		sample.__isset_bitfield = 2;
		adaptor.getNulls(sample, dst);

		// when & then
		Assert.assertEquals("{1}", dst.toString());
	}

	@Test
	public void testGetLongBitField() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<LongFieldSample> adaptor = new ThriftNullsAdaptor<>(LongFieldSample.class);
		BitSet dst = new BitSet();

		LongFieldSample sample = new LongFieldSample();
		sample.__isset_bitfield = 2;
		adaptor.getNulls(sample, dst);

		// when & then
		Assert.assertEquals("{1}", dst.toString());
	}

	@Test
	public void testGetBitSet() throws JournalConfigurationException {

		// given
		ThriftNullsAdaptor<BitSetSample> adaptor = new ThriftNullsAdaptor<>(BitSetSample.class);
		BitSet dst = new BitSet();

		BitSetSample sample = new BitSetSample();
		sample.__isset_bit_vector.set(1);
		adaptor.getNulls(sample, dst);

		// when & then
		Assert.assertEquals("{1}", dst.toString());
	}

	private static class ByteFieldSample {
		private byte __isset_bitfield = 16;
		private String s1;
		private int a;
		private String s2;
		private long b;
	}

	private static class ShortFieldSample {
		private short __isset_bitfield = 0;
		private String s1;
		private int a;
		private String s2;
		private long b;
	}

	private static class IntFieldSample {
		private int __isset_bitfield = 0;
		private String s1;
		private int a;
		private String s2;
		private long b;
	}

	private static class LongFieldSample {
		private int __isset_bitfield = 16;
		private String s1;
		private int a;
		private String s2;
		private long b;
	}

	private static class BitSetSample {
		private final BitSet __isset_bit_vector = new BitSet(2);
		private String s1;
		private int a;
		private String s2;
		private long b;
	}

}