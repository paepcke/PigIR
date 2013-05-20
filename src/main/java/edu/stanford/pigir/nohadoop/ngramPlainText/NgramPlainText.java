/**
 * Takes a plain text file, and generates an output file with 
 * ngrams and their probabilities, smoothed via Good-Turing.
 * The arity of ngrams maybe be specified.
 * Example:
 * 		NgramPlainText ngrammer = new NgramPlainText("src/test/resources/ClueWeb09_English_Sample.warc", 
 *													 "/tmp/ngramTest.csv", 
 *													 3); // I.e. trigrams. Use 2 for bigrams, etc.
 *      produces a file like this:
 *  	   2.822000e-06,More,people,have
 *  	   2.822000e-06,to,Sakura,Friend
 *  	   2.822000e-06,Sailing,Opportunities,WSO
 *                    ...
 *                    
 * You must run the code in the project root directory.
 * Note that no HTML detagging is done by this module.                          
 */

package edu.stanford.pigir.nohadoop.ngramPlainText;

import static ch.lambdaj.Lambda.by;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.lambdaj.Lambda;
import ch.lambdaj.group.GroupItem;

import com.google.common.io.Files;

import edu.stanford.pigir.nohadoop.TokenizeRegExp;
import edu.stanford.pigir.pigudf.NGramUtil;

/**
 * Comparator for sorting funky data structures produced
 * by a group-by capability library. 
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

/**
 * Main class
 *
 */
public class NgramPlainText {
	
	static boolean REMOVE_DUP_NGRAMS = true;
	static boolean KEEP_DUP_NGRAMS = false;


	/**
	 * This constructor provides a version that
	 * requires a minimal number of parameters. 
	 * @param plainTextFileName Full path of input text file. 
	 * @param outfileName Full path of file where the probabilities and ngrams will be deposited 
	 * @param arity Arity of the ngrams.
	 * @throws IOException
	 */
	public NgramPlainText(String plainTextFileName, String outfileName, int arity) throws IOException {
		generateNgrams(new File(plainTextFileName), new File(outfileName), arity,  NgramPlainText.KEEP_DUP_NGRAMS);
	}

	/**
	 * This constructor provides a version that
	 * requires a minimal number of parameters. 
	 * @param plainTextFileName Full path of input text file. 
	 * @param outfileName Full path of file where the probabilities and ngrams will be deposited 
	 * @param arity Arity of the ngrams.
	 * @throws IOException
	 */
	public NgramPlainText(File plainTextFile, String outfileName, int arity) throws IOException {
		generateNgrams(plainTextFile, new File(outfileName), arity, NgramPlainText.KEEP_DUP_NGRAMS);
	}
	
	/**
	 * This constructor provides a version that
	 * requires a minimal number of parameters. 
	 * @param plainTextFileName Full path of input text file. 
	 * @param outfileName Full path of file where the probabilities and ngrams will be deposited 
	 * @param arity Arity of the ngrams.
	 * @throws IOException
	 */
	public NgramPlainText(File plainTextFile, File outfileName, int arity) throws IOException {
		generateNgrams(plainTextFile, outfileName, arity, NgramPlainText.KEEP_DUP_NGRAMS);
	}
	
	/**
	 * Main initializer.
	 * @param infile Full path of input text file.
	 * @param outfile Full path of file where the probabilities and ngrams will be deposited
	 * @param arity Arity of the ngrams.
	 * @param removeDups If removeDups is NgramPlainText.REMOVE_DUP_NGRAMS, then each ngram is only represented
	 *                   once! This duplicate suppression makes the probabilities meaningless. But the
	 *                   option may be useful if only the ngrams are desired, and the probabilities will
	 *                   be ignored.
	 * @throws IOException
	 */
	public static void generateNgrams(File infile, File resOutfile, int arity, boolean removeDups) throws IOException {
		if (! infile.canRead()) {
			throw new IOException(String.format("Input text file '%s' does not exist or cannot be opened.", infile));
		}

		try {
			resOutfile.createNewFile();
		} catch (IOException e) {
			throw new IOException(String.format("Output ngram file '%s' cannot be created: %s", resOutfile, e.getMessage()));
		}
		
		// Generate the ngrams from the input file:
		Collection<String> ngrams = createNgrams(infile, arity, removeDups);

		// Get ngram-->count mappings:
		Map<String,Integer> ngramsPlusCounts = NgramPlainText.countNgrams(ngrams);
		
		// Get frequency of frequencies:
		Map<Integer,Integer> freqsOfFreqs = NgramPlainText.getFreqOfFreqs(ngramsPlusCounts);
		
		// Apply Good-Turing smoothing to freqOfFreqs:
		Map<Integer,Float> freqPlusProbability = NgramPlainText.applyGoodTuring(freqsOfFreqs);
		
		// Now we have ngram-->count, and we have count-->probability
		// Need probability,ngram:
		FileWriter fileWriter = new FileWriter(resOutfile);
		BufferedWriter outStream = new BufferedWriter(fileWriter);
		for (String ngram : ngramsPlusCounts.keySet()) {
			int ngramCount = ngramsPlusCounts.get(ngram);
			float ngramCountProbability = freqPlusProbability.get(ngramCount);
			String tuple = String.format("%e,%s\n", ngramCountProbability, ngram);
			outStream.write(tuple);
		}
		outStream.close();
	}
	
	// Declared public to make accessible to unit tests:
	//private static Map<String,Integer> countNgrams(Collection<String> ngrams) {
	public static Map<String,Integer> countNgrams(Collection<String> ngrams) {
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
	
	/**
	 * This method performs a group-by over an ngram-->ngramOccurrenceCount map.
	 * We need this grouping to compute the number of times each count was observed.
	 * The feature is provided by lambdaj:
	 * http://lambdaj.googlecode.com/svn/trunk/html/apidocs/ch/lambdaj/group/Group.html
	 * @param ngramsPlusFreqs the map from ngram-->thatNgram'sCount
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
		// Need an entry for one occurrence to make Good Turing set
		// aside some probability mass for unseen ngrams:
		if (freqToFreqOfFreqs.get(1) == null) {
			freqToFreqOfFreqs.put(1,1);
		}
		
		return freqToFreqOfFreqs;
	}
	
	/**
	 * Takes a frequency-->numberOfObservationsOfThisFrequency map.
	 * Invokes a C function that computes a map count-->probability
	 * via Good-Turing smoothing:
	 * @param freqToFreqOfFreqs
	 * @return map ngramCount-->probabilty
	 * @throws IOException
	 */
	private static Map<Integer,Float> applyGoodTuring(Map<Integer,Integer> freqToFreqOfFreqs) throws IOException {
		Set<Integer> freqs = freqToFreqOfFreqs.keySet();
		List<Integer> freqsList = new ArrayList<Integer>();
		freqsList.addAll(freqs);
		Collections.sort(freqsList);

		// Write freqToFreqOfFreq to a temp file:
		File freqOfFreqsFile = File.createTempFile("freqToFreqOfFreqsTmp", ".csv");
		freqOfFreqsFile.deleteOnExit();
		FileWriter fileoutput = new FileWriter(freqOfFreqsFile);
		BufferedWriter buffout = new BufferedWriter(fileoutput);		
		
		// Do the writing:
		for (Integer freq : freqsList) {
			Integer freqOfFreq = freqToFreqOfFreqs.get(freq);
			String tupleToWrite= freq.toString() + "," + freqOfFreq.toString() + "\n";
			buffout.write(tupleToWrite);
		}
		buffout.close();
		
		// Get second tmpfile for good turing smoother to write to:
		File smoothedResultFile = File.createTempFile("smoothedResultTmp", ".csv");
		smoothedResultFile.deleteOnExit();
		
		File execFile = new File(System.getProperty("user.dir") + "/src/main/scripts/runGoodTuring.sh");
		if (!execFile.exists()) {
			System.out.println("Exec file does not exist: " + execFile.getAbsolutePath());
		}
		
		if (!execFile.canExecute()) {
			System.out.println("Exec file not executable: " + execFile.getAbsolutePath());
		}
		
		if (!freqOfFreqsFile.exists()) {
			System.out.println("Input file does not exist: " + freqOfFreqsFile.getAbsolutePath());
		}
		if (!smoothedResultFile.exists()) {
			System.out.println("Output file does not exist: " + smoothedResultFile.getAbsolutePath());
		}
		
		String cmdLineStr = System.getProperty("user.dir") + "/src/main/scripts/runGoodTuring.sh " + freqOfFreqsFile.getAbsolutePath() + " " + smoothedResultFile.getAbsolutePath();
		Runtime.getRuntime().exec(cmdLineStr);
		
/*		CommandLine cmdLine = new CommandLine(cmdLineStr);
		//CommandLine cmdLine = parser.parse(new Options(), cmdLineArr);
		DefaultExecutor executor = new DefaultExecutor();
		executor.setExitValue(1);
		@SuppressWarnings("unused")
		int exitValue = executor.execute(cmdLine);
*/		
		// Read good turing file back in, and make count-->probability Map
		Map<Integer,Float> countToProbMap = new HashMap<Integer,Float>();
		Charset charset = Charset.forName("US-ASCII");
		//****try (BufferedReader reader = Files.newReader(smoothedResultFile, charset)) {
		BufferedReader reader = null;
		try {
			reader = Files.newReader(smoothedResultFile, charset);
			String line = null;
			while ((line = reader.readLine()) != null) {
				//System.out.println(line);
				String[] countAndProb = line.split(",");
				if (countAndProb[0].length() == 0) {
					// Most likely, the Good-Turing algo had too little input,
					// and the result file has a leading empty line, followed
					// by "Fewer than 5 input value-pairs":
					String errorMsg = reader.readLine();
					throw new IOException(errorMsg);
				}
				countToProbMap.put(Integer.valueOf(countAndProb[0]), Float.valueOf(countAndProb[1]));
			}
		} catch (IOException x) {
			System.err.format("IOException: %s%n", x);
			throw new IOException(x.getMessage());
		} finally {
			reader.close();
		}
		

		return countToProbMap;
	}
	
	/**
	 * Workhorse for chopping text file into ngrams
	 * @param infile Plain text file.
	 * @param arity ngram arity
	 * @param removeDups whether to keep ngram duplicates (the usual case) 
	 * @return ngrams
	 * @throws IOException
	 */
	private static Collection<String> createNgrams(File infile, int arity, boolean removeDups) throws IOException {	
		String text = readFile(infile);
		List<String> tokens = new TokenizeRegExp().tokenize(text);
		Collection<String> ngrams = null;
		if (removeDups == NgramPlainText.KEEP_DUP_NGRAMS)
			ngrams = new ArrayList<String>();
		else
			ngrams = new HashSet<String>();
		NGramUtil.makeNGram(tokens.toArray(new String[tokens.size()]), ngrams, arity, NGramUtil.NOT_ALL_NGRAMS);
		return ngrams;
	}
	
	
	private static String readFile(File infile) throws IOException {
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
	
	// ======================= Main =========================
	
	public static void main(String[] args) throws IOException {
		
		if (args.length < 3) {
			System.out.println("Usage: NgramPlainText <infile:String> <outfile:String> <arity:int>");
			System.exit(1);
		}
		
		new NgramPlainText(args[0], args[1], Integer.valueOf(args[2]));
		
		
	// =====================================   Just for  Testing =================
		
	}
}
