/**
 * 
 */
package edu.stanford.pigir.nohadoop.ngramPlainText;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import edu.stanford.pigir.nohadoop.TokenizeRegExp;
import edu.stanford.pigir.pigudf.NGramUtil;

/**
 * @author paepcke
 *
 */
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
	
	public void generateNgrams(File infile, File outfile, int arity, boolean removeDups) throws IOException {
		if (! infile.canRead()) {
			throw new IOException(String.format("Input text file '%s' does not exist or cannot be opened.", infile));
		}

		try {
			outfile.createNewFile();
		} catch (IOException e) {
			throw new IOException(String.format("Output ngram file '%s' cannot be created: %s", outfile, e.getMessage()));
		}
		
		BufferedWriter outStream = new BufferedWriter(new FileWriter(outfile));
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

}
