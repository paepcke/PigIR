package edu.stanford.pigir.nohadoop;

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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

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
		// int arity = 2;
		ArrayList<String> input = new ArrayList<String>();
		input.add("here,here");
		input.add("are,are");                     
		input.add("are,are");
		input.add("again,yes");
		input.add("here,here");
		input.add("Happy,days");
		input.add("yes,yes");
		input.add("yes,yes");
		input.add("are,here");
		input.add("yes,yes");
		input.add("here,here");
		input.add("here,here");
		input.add("days,are");
		input.add("here,here");
		input.add("here,again");
		input.add("yes,yes");
		input.add("yes,yes");
		input.add("are,are");
		input.add("yes,yes");
		
		Map<String,Integer> groundTruth = new HashMap<String,Integer>();
		groundTruth.put("Happy,days", 1);
		groundTruth.put("days,are", 1);
		groundTruth.put("are,are", 3);
		groundTruth.put("are,here", 1);
		groundTruth.put("here,here", 5);
		groundTruth.put("here,again", 1);
		groundTruth.put("again,yes", 1);
		groundTruth.put("yes,yes", 6);
		
		Map<String,Integer> result = NgramPlainText.countNgrams(input);
		assertTrue(compareMaps(groundTruth, result));
	}
	
	/**
	 * Using src/test/resources/plainTextTestFile.txt, test ngram smoothing from
	 * text to final frequency-->probability. The text file generates the following
	 * frequency of frequencies:
	 * 1,5
	   3,1
	   4,1
	   5,1
	   6,1
	   
	   The final result should be (by goodTuringSmoothing.c):
	   0,0.2174
	   1,0.03824
	   3,0.1013
	   4,0.1324
	   5,0.1634
	   6,0.1944

	 * @throws IOException
	 */
	@Test
	public void testGenerateNgramCountsViaFiles() throws IOException {
		int arity = 2;
		
		String[] groundTruth = new String[] {"3.824000e-02,again,yes",
						        			 "1.634000e-01,again,again",                      
						        			 "1.013000e-01,are,are",
						        			 "1.944000e-01,yes,yes",
						        			 "1.324000e-01,here,here",
						        			 "3.824000e-02,here,again",
						        			 "3.824000e-02,Happy,days",
						        			 "3.824000e-02,are,here",
						        			 "3.824000e-02,days,are"
		};
		// Read input file, and write ngram probability to output file:
		new NgramPlainText(plainTextTestFile, tmpFile, arity);
		String[] resultLines = readLines(tmpFile.getAbsolutePath());
		assertArrayEquals("Ngram smoothing for plainTextTestFile failed.", groundTruth, resultLines);
	}
	

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
