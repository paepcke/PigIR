package edu.stanford.pigir.nohadoop;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.Test;

import edu.stanford.pigir.pigudf.NGramUtil;

public class TestNGramUtil {

	@Test
	public void testBigramSets() throws IOException {
		
		String[] words = {"This", "is", "a", "juicy", "test", "a", "juicy", "test"};
		Collection<String> ngrams = new HashSet<String>();
		int arity = 2;
		NGramUtil.makeNGram(words, ngrams, arity);
		assertTrue(compareColls(Arrays.asList("This,is", "is,a", "a,juicy", "juicy,test", "test,a"),
				   				ngrams));
	}

	@Test
	public void testTrigramsSets() throws IOException {
		String[] words = {"This", "is", "a", "juicy", "test", "a", "juicy", "test"};
		Collection<String> ngrams = new HashSet<String>();
		int arity = 3;
		NGramUtil.makeNGram(words, ngrams, arity);
		assertTrue(compareColls(Arrays.asList("This,is,a", "is,a,juicy", "juicy,test,a", "test,a,juicy", "a,juicy,test"),
								ngrams));
	}

	
	@Test
	public void testBigramCollection() throws IOException {
		
		String[] words = {"This", "is", "a", "juicy", "test", "a", "juicy", "test"};
		Collection<String> ngrams = new ArrayList<String>();
		int arity = 2;
		NGramUtil.makeNGram(words, ngrams, arity);
		assertTrue(compareColls(Arrays.asList("This,is", "is,a", "a,juicy", "juicy,test", "test,a", "a,juicy", "juicy,test"),
				   				ngrams));
	}
	
	
	@Test
	public void testTrigramCollection() throws IOException {
		
		String[] words = {"This", "is", "a", "juicy", "test", "a", "juicy", "test"};
		Collection<String> ngrams = new ArrayList<String>();
		int arity = 3;
		NGramUtil.makeNGram(words, ngrams, arity);
		assertTrue(compareColls(Arrays.asList("This,is,a", "is,a,juicy", "a,juicy,test", "juicy,test,a", "test,a,juicy", "a,juicy,test"),
				   				ngrams));
	}
	
	private boolean compareColls(Collection<String> coll1, Collection<String> coll2) {
		
		if (coll1.size() != coll2.size())
			return false;
		
		HashMap<String,Integer> coll1Map = new HashMap<String,Integer>();
		HashMap<String,Integer> coll2Map = new HashMap<String,Integer>();
		
		for (String collStr1 : coll1) {
				Integer elCount1 = coll1Map.get(collStr1);
				if (elCount1 == null) {
					coll1Map.put(collStr1, 1);
				} else {
					coll1Map.put(collStr1, elCount1 + 1);
				}
		}
		for (String collStr2 : coll2) {
				Integer elCount2 = coll2Map.get(collStr2);
				if (elCount2 == null) {
					coll2Map.put(collStr2, 1);
				} else {
					coll2Map.put(collStr2, elCount2 + 1);
				}
		}
		
		if (coll1Map.size() != coll2Map.size())
			return false;
		for (String key : coll1Map.keySet()) {
			if (coll1Map.get(key) != coll2Map.get(key))
				return false;
		}
		return true;
	}

}
