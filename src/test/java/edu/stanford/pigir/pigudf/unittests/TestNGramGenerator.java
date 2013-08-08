package edu.stanford.pigir.pigudf.unittests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.SortedDataBag;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.pigudf.NGramGenerator;

public class TestNGramGenerator {

	public static BinSedesTupleFactory tupleFac = null;
	Tuple parmLen1 = null;
	Tuple parmLen2 = null;
	NGramGenerator ngramFunc = null;
	
	static class TupleFac{
		public static Tuple newTuple(String[] content) throws ExecException {
			Tuple t = TestNGramGenerator.tupleFac.newTuple(content.length);
			for (int i=0; i<content.length; i++)
				t.set(i, content[i]);
			return t;
		}
	}
	
	@Before
	public void setUp() throws Exception {
		tupleFac = new BinSedesTupleFactory();
		parmLen1 = tupleFac.newTuple(1);
		parmLen2 = tupleFac.newTuple(2);
		ngramFunc = new NGramGenerator();
	}
	
	/**
	 * Given two tuple bags as returned by the NGramGenerator function,
	 * return true if the two bags contain the same number of tuples, which,
	 * pairwise, have the same contents. Strategy, sort the bags, and then
	 * compare tuple by tuple.
	 * @param bag1
	 * @param bag2
	 * @return
	 * @throws ExecException
	 */
	private boolean compareBags(DefaultDataBag bag1, DefaultDataBag bag2) throws ExecException {
		SortedDataBag sortedBag1 = new SortedDataBag(null);
		SortedDataBag sortedBag2 = new SortedDataBag(null);
		sortedBag1.addAll(bag1);
		sortedBag2.addAll(bag2);
		Iterator<Tuple> bag1Iter = sortedBag1.iterator();
		Iterator<Tuple> bag2Iter = sortedBag2.iterator();
		
		while (bag1Iter.hasNext()) {
			if (! bag2Iter.hasNext()) {
				return false;
			}
			Tuple t1 = bag1Iter.next();
			Tuple t2 = bag2Iter.next();
			//************
			//int t1Size = t1.size();
			//int t2Size = t2.size();
			//************
			if (t1.size() != t2.size())
				return false;
			for (int i=0; i<t1.size(); i++) {
				if (! t1.get(i).equals(t2.get(i)))
					return false;
			}
		}
		if (bag2Iter.hasNext())
			return false;
		return true;
	}

	private DefaultDataBag makeBag(Tuple[] tuples) throws ExecException {
		DefaultDataBag bag = new DefaultDataBag();
		for (Tuple tuple : tuples)
			bag.add(tuple);
		return bag;
	}

	@Test
	public void testMakeBag() throws ExecException {
		Tuple t1 = TupleFac.newTuple(new String[]{"Foo", "Bar"});
		Tuple t2 = TupleFac.newTuple(new String[]{"Blue", "Green"});		
		DefaultDataBag bag = makeBag(new Tuple[]{t1,t2});
		
		DefaultDataBag manualBag = new DefaultDataBag();
		manualBag.add(t1);
		manualBag.add(t2);
		
		assertTrue(compareBags(manualBag, bag));
	}
	
	@Test
	public void testCompareBags() throws ExecException {
		// Test compareBags:
		DefaultDataBag bag1 = new DefaultDataBag();
		DefaultDataBag bag2 = new DefaultDataBag();
		
		// Test bag with one tuple of len 1:
		Tuple t1_len1 = tupleFac.newTuple(1);
		Tuple t2_len1 = tupleFac.newTuple(1);
		t1_len1.set(0, "foo");
		t2_len1.set(0, "foo");
		bag1.add(t1_len1);
		bag2.add(t2_len1);
		assertTrue(compareBags(bag1, bag2));
		t1_len1.set(0, "bar");
		assertFalse(compareBags(bag1, bag2));
		
		// Test bag with one tuple of len 2:
		bag1.clear();
		bag2.clear();
		Tuple t1_len2 = tupleFac.newTuple(2);
		Tuple t2_len2 = tupleFac.newTuple(2);
		t1_len2.set(0, "foo");
		t1_len2.set(1, "bar");
		t2_len2.set(0, "foo");
		t2_len2.set(1, "bar");
		bag1.add(t1_len2);
		bag2.add(t2_len2);
		assertTrue(compareBags(bag1, bag2));		
		
		// Test unequal number of tuples in the two bags:
		Tuple t3_len2 = tupleFac.newTuple(2);
		t3_len2.set(0, "foo");
		t3_len2.set(1, "bar");
		bag1.add(t3_len2);
		assertFalse(compareBags(bag1, bag2));		
	}
	
	@Test
	public void testBigrams() throws IOException {
		
		parmLen1.set(0, "This is a juicy test.");
		DefaultDataBag resBag = makeBag(new Tuple[] {
				TupleFac.newTuple(new String[] {"juicy,test"}),
				TupleFac.newTuple(new String[] {"is,a"}),
				TupleFac.newTuple(new String[] {"This,is"}),
				TupleFac.newTuple(new String[] {"a,juicy"}),
		});
		assertTrue(compareBags(resBag, (DefaultDataBag) ngramFunc.exec(parmLen1)));
	}

	@Test
	public void testBigramsWithDupsNotRemoved() throws IOException {
		
		parmLen1.set(0, "This is a juicy test, a juicy test.");
		DefaultDataBag resBag = makeBag(new Tuple[] {
				TupleFac.newTuple(new String[] {"juicy,test"}),
				TupleFac.newTuple(new String[] {"is,a"}),
				TupleFac.newTuple(new String[] {"This,is"}),
				TupleFac.newTuple(new String[] {"a,juicy"}),
				TupleFac.newTuple(new String[] {"test,a"}),
				TupleFac.newTuple(new String[] {"a,juicy"}),
				TupleFac.newTuple(new String[] {"juicy,test"}),
		});
		assertTrue(compareBags(resBag, (DefaultDataBag) ngramFunc.exec(parmLen1)));
	}

	
	@Test
	public void testTrigrams() throws IOException {
		parmLen2.set(0, "This is a nice juicy test.");		
		parmLen2.set(1, 3); // 3: trigrams		
		DefaultDataBag resBag = makeBag(new Tuple[] {
				TupleFac.newTuple(new String[] {"nice,juicy,test"}),
				TupleFac.newTuple(new String[] {"This,is,a"}),
				TupleFac.newTuple(new String[] {"is,a,nice"}),
				TupleFac.newTuple(new String[] {"a,nice,juicy"}),
		});
		assertTrue(compareBags(resBag, (DefaultDataBag) ngramFunc.exec(parmLen2)));
	}

	public void testTrigramsWithDupsNotRemoved() throws IOException {
		parmLen2.set(0, "This is a nice juicy test, a juicy test");		
		parmLen2.set(1, 3); // 3: trigrams		
		DefaultDataBag resBag = makeBag(new Tuple[] {
				TupleFac.newTuple(new String[] {"nice,juicy,test"}),
				TupleFac.newTuple(new String[] {"This,is,a"}),
				TupleFac.newTuple(new String[] {"is,a,nice"}),
				TupleFac.newTuple(new String[] {"a,nice,juicy"}),
				TupleFac.newTuple(new String[] {"juicy,test,a"}),
				TupleFac.newTuple(new String[] {"test,a,juicy"}),
				TupleFac.newTuple(new String[] {"a,juicy,test"}),
		});
		assertTrue(compareBags(resBag, (DefaultDataBag) ngramFunc.exec(parmLen2)));
	}

}
