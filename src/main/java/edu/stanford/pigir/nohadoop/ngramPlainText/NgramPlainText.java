/**
 * 
 */
package edu.stanford.pigir.nohadoop.ngramPlainText;

import static ch.lambdaj.Lambda.by;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.lambdaj.Lambda;
import ch.lambdaj.group.GroupItem;
import edu.stanford.pigir.nohadoop.TokenizeRegExp;
import edu.stanford.pigir.pigudf.NGramUtil;

/**
 * @author paepcke
 *
 */

class FreqToOccurrenceListComparator implements Comparator<GroupItem<Map<Integer,List<Integer>>>> {
	
	@SuppressWarnings("unchecked")
	public int compare(GroupItem<Map<Integer,List<Integer>>> item1, 
					   GroupItem<Map<Integer,List<Integer>>> item2) {
		Integer freq1 = ((List<Integer>)item1.get("children")).get(0);
		Integer freq2 = ((List<Integer>)item2.get("children")).get(0);
		if (freq1 > freq2) 
			return 1;
		else if (freq1 == freq2) 
			return 0;
		else
			return -1;
	}
}

public class NgramPlainText {
	
	static boolean REMOVE_DUP_NGRAMS = true;
	static boolean KEEP_DUP_NGRAMS = false;


	/**
	 * @param args
	 * @throws IOException 
	 */
	
	public NgramPlainText(String plainTextFileName, String outfileName, int arity) throws IOException {
		generateNgrams(new File(plainTextFileName), new File(outfileName), arity,  NgramPlainText.KEEP_DUP_NGRAMS);
	}

	public NgramPlainText(File plainTextFile, String outfileName, int arity) throws IOException {
		generateNgrams(plainTextFile, new File(outfileName), arity, NgramPlainText.KEEP_DUP_NGRAMS);
	}
	
	public NgramPlainText(File plainTextFile, File outfileName, int arity) throws IOException {
		generateNgrams(plainTextFile, outfileName, arity, NgramPlainText.KEEP_DUP_NGRAMS);
	}
	
	/**
	 * Main initializer.
	 * @param infile
	 * @param outfile
	 * @param arity
	 * @param removeDups
	 * @throws IOException
	 */
	public void generateNgrams(File infile, File resOutfile, int arity, boolean removeDups) throws IOException {
		if (! infile.canRead()) {
			throw new IOException(String.format("Input text file '%s' does not exist or cannot be opened.", infile));
		}

		try {
			resOutfile.createNewFile();
		} catch (IOException e) {
			throw new IOException(String.format("Output ngram file '%s' cannot be created: %s", resOutfile, e.getMessage()));
		}
		BufferedWriter outStream = new BufferedWriter(new FileWriter(resOutfile));
		writeNgrams(infile, outStream, arity, removeDups);
	}
	
	
	public Map<String,Integer> countNgrams(String[] ngrams) {
		return countNgrams(new ArrayList<String>(Arrays.asList(ngrams)));
	}
		
	public Map<String,Integer> countNgrams(Collection<String> ngrams) {
		Map<String,Integer> res = new HashMap<String,Integer>(); 
		for (String ngram : ngrams) {
				Integer ngramCount = res.get(ngram);
				if (ngramCount == null) {
					res.put(ngram, 1);
				} else {
					res.put(ngram, ngramCount + 1);
				}
		}
		return res;
	}
	
	
	//************* NEXT: integrate smoothing
	
	/**
	 * http://lambdaj.googlecode.com/svn/trunk/html/apidocs/ch/lambdaj/group/Group.html
	 * @param ngramsPlusFreqs
	 * @return
	 */
	private static Map<Integer,Integer> getFreqOfFreqs(Map<String,Integer> ngramsPlusFreqs) {
		
		@SuppressWarnings("unchecked")
		// Get a map from observed ngram counts to a list the size of those counts.
		// Ex: if one ngram was seen 3 times, and another was seen once, the result
		//     of the following statement would look like this map:
		//        3-->[3,3,3]
		//        1-->[1,1,1,1,1,1,1]
		// Note that the values of this map are of type List, so you need
		// to use get() to extract values:
		
		List<GroupItem<Map<Integer,List<Integer>>>> groupFreqToCounts = 
				(List<GroupItem<Map<Integer,List<Integer>>>>) Lambda.group(ngramsPlusFreqs.values(), by(Lambda.on(Integer.class).intValue()));
		
		// From the GroupItems, get our frequency --> freqOfFreq map.
		// Continuing the above example, get:
		//         1-->7
		//         3-->3
		Map<Integer,Integer> freqToFreqOfFreqs = new HashMap<Integer,Integer>(); 
		for (GroupItem<Map<Integer,List<Integer>>> ngramCountGroup : groupFreqToCounts) {
			@SuppressWarnings("unchecked")
			List<Integer> listOfCounts = (List<Integer>) ngramCountGroup.get("children");
			Integer freq = listOfCounts.get(0);
			freqToFreqOfFreqs.put(freq, listOfCounts.size());
		}
		// Need an entry for one occurrence to make Good Turing work.
		if (freqToFreqOfFreqs.get(1) == null) {
			freqToFreqOfFreqs.put(1,1);
		}
		
		return freqToFreqOfFreqs;
	}
	
	private static Map<Integer,Float> applyGoodTuring(Map<Integer,Integer> freqToFreqOfFreqs) throws FileNotFoundException {
		PrintStream streamToGoodTuring = new PrintStream(new FileOutputStream("bin/goodTuringSmoothing"));
		Set<Integer> freqs = freqToFreqOfFreqs.keySet();
		List<Integer> freqsList = new ArrayList<Integer>();
		freqsList.addAll(freqs);
		Collections.sort(freqsList);
		
		// Build strings: "freq,freqOfFreq\n" to feed into the 
		// C binary goodTuringSmoothing:
		for (Integer freq : freqsList) {
			Integer freqOfFreq = freqToFreqOfFreqs.get(freq);
			String tupleToPass = freq.toString() + "," + freqOfFreq.toString() + "\n";
			streamToGoodTuring.print(tupleToPass);
		}
		streamToGoodTuring.close();
		return null; //*******
	}
	
	private void writeNgrams(File infile, BufferedWriter outfileStream, int arity, boolean removeDups) throws IOException {	
		String text = readFile(infile);
		List<String> tokens = new TokenizeRegExp().tokenize(text);
		Collection<String> ngrams = null;
		if (removeDups == NgramPlainText.KEEP_DUP_NGRAMS)
			ngrams = new ArrayList<String>();
		else
			ngrams = new HashSet<String>();
		NGramUtil.makeNGram(tokens.toArray(new String[tokens.size()]), ngrams, arity, NGramUtil.NOT_ALL_NGRAMS);
		for (String ngram : ngrams) {
			outfileStream.write(ngram + '\n');
		}
		outfileStream.close();
	}
	
	
	private String readFile(File infile) throws IOException {
	  FileInputStream stream = new FileInputStream(infile);
	  try {
	    FileChannel fc = stream.getChannel();
	    MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
	    /* Instead of using default, pass in a decoder. */
	    return Charset.defaultCharset().decode(bb).toString();
	  }
	  finally {
	    stream.close();
	  }
	}	
	
	public static void main(String[] args) throws FileNotFoundException {
		Map<String,Integer> ngramCounts = new HashMap<String,Integer>();
		ngramCounts.put("again,Happy",4); 	
		ngramCounts.put("here,again",1);  	
		ngramCounts.put("Happy,days",2);  	
		ngramCounts.put("are,here",1);		
		ngramCounts.put("days,are",1);    	
		
		Map<Integer,Integer> freqOfFreqs = NgramPlainText.getFreqOfFreqs(ngramCounts);
		System.out.println(freqOfFreqs);
		applyGoodTuring(freqOfFreqs);
/*		for (GroupItem<Map<Integer,List<Integer>>> ngramCountGroup : ngramCountGroups) {
			@SuppressWarnings("unchecked")
			List<Integer> listOfCounts = (List<Integer>) ngramCountGroup.get("children");
			Integer freq = listOfCounts.get(0);
			System.out.println(freq + "," + listOfCounts.size());
		}
*/	}
}
