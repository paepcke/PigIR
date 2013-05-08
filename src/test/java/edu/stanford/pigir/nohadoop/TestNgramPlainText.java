package edu.stanford.pigir.nohadoop;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.nohadoop.ngramPlainText.NgramPlainText;

// Note that creating the ngrams is tested in TestNGramUtil.java.
// Here we focus on counting.


public class TestNgramPlainText {
	
	List<String> ngrams = new ArrayList<String>();
	File tmpFile = null;
	File plainTextTestFile = new File("src/test/resources/plainText.txt");

	@Before
	public void setUp() throws IOException {
		ngrams = Arrays.asList("This,is", "is,a", "a,juicy", "juicy,test", "test,a", "a,juicy", "juicy,test");
		tmpFile = File.createTempFile("tmp",".txt");
		tmpFile.deleteOnExit();
	}

	@Test
	public void testCountNgrams() throws IOException {
		int arity = 2;
		NgramPlainText ngrammer = new NgramPlainText(plainTextTestFile, tmpFile, arity);
		
		Map<String,Integer> groundTruth = new HashMap<String,Integer>();
		groundTruth.put("This,is", 1);
		groundTruth.put("is,a",1);
		groundTruth.put("a,juicy",2);
		groundTruth.put("juicy,test",2);
		groundTruth.put("test,a",1);
		
		assertTrue(compareMaps(groundTruth, ngrammer.countNgrams(ngrams)));
	}
	
	@Test
	public void testGenerateNgramCountsViaFiles() throws IOException {
		int arity = 2;
		Map<String,Integer> groundTruth = new HashMap<String,Integer>();
		groundTruth.put("again,Happy",1); 	
		groundTruth.put("here,again",1);  	
		groundTruth.put("Happy,days",2);  	
		groundTruth.put("are,here",1);		
		groundTruth.put("days,are",1);    	
						
		// Read input file, and write ngrams to output file:
		NgramPlainText ngrammer = new NgramPlainText(plainTextTestFile, tmpFile, arity);
		String[] resultLines = readLines(tmpFile.getAbsolutePath());
		//for (String line : resultLines)
		//	System.out.println(line);
		Map<String,Integer> ngramCounts = ngrammer.countNgrams(resultLines);
		//for (String line : ngramCounts.keySet()) {
		//	System.out.println(line + ":" + ngramCounts.get(line));
		//}
		assertTrue(compareMaps(groundTruth, ngramCounts));
		
	}
	
//************* NEXT: Integrate smoothing	
	
	private boolean compareMaps(Map<String,Integer> map1, Map<String,Integer> map2) {
		if (map1.size() != map2.size())
			return false;
		for (String key : map1.keySet()) {
			Integer elCount1 = map1.get(key);
			Integer elCount2 = map2.get(key);
			if (elCount1 != elCount2) {
				return false;
			}
		}
		return true;
	}
	
	public String[] readLines(String filename) throws IOException {
		FileReader fileReader = new FileReader(filename);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		List<String> lines = new ArrayList<String>();
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			lines.add(line);
		}
		bufferedReader.close();
		return lines.toArray(new String[lines.size()]);
	}
}	
