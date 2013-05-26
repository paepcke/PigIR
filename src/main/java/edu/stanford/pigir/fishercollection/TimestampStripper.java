package edu.stanford.pigir.fishercollection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;

import edu.stanford.pigir.warc.WarcFromTextFileMaker;

/**
 * @author paepcke
 *
 * Create WARC files from Fisher Collection files. Each Fisher file will be
 * one gzipped WARC file with one record. Its record header will be:
  			WARC_RECORD_ID
			CONTENT_LENGTH
			WARC_DATE
			WARC_TYPE
			
 * Fisher files look like this:
		26.62 28.05 A: i think it was 
		
		28.13 29.66 A: gossiping 
		
		29.06 30.29 B: gossiping 
		
		30.30 31.18 B: (( mm )) 
		
		31.84 33.42 B: probably smoking 
		
		33.14 34.30 A: smoking 

 * The files will have sentences alternating by conversation partner, with
 * timestamp and partner ID removed:
 * 
       i think it was gossiping
       (( mm  )) probably smoking
       smoking
 */
public class TimestampStripper {

	public TimestampStripper(String directoryRoot, String targetDir) throws IOException {
		this(directoryRoot, targetDir, 0);
	}
	
	public TimestampStripper(String directoryRoot, String targetDir, int outfileSerialNumStart) throws IOException {
		
		WarcFromTextFileMaker warcMaker = new WarcFromTextFileMaker(directoryRoot);
		
		while (warcMaker.hasNext()) {
			File infile = warcMaker.next();
			List<String> allUtterances = FileUtils.readLines(infile);
			Collection<String> flatFileLines =  stripTimestampsAndFlatten(allUtterances);
			// Make an output path:
			String outfileName = infile.getName() + "_" + Integer.toString(outfileSerialNumStart);
			File outfile = new File(targetDir, outfileName);
			Collection<String> warcHeader = warcMaker.makeWarcHeader(infile.getName(), flatFileLines);
			FileUtils.writeLines(outfile, warcHeader);
			FileUtils.writeLines(outfile, flatFileLines, true);
		}
	}
	
	private Collection<String> stripTimestampsAndFlatten(List<String> allUtterances) {
		ArrayList<String> result = new ArrayList<String>();
		char currSpeaker = 'A';
		String currSentence = "";
		for (String oneLine : allUtterances) {
			// Get [<timestamp> <speaker, text]: 
			String[] timestampPlusUtterance = oneLine.split(":");
			if (timestampPlusUtterance.length < 2)
				// Empty line or file header line:
				continue;
			// Did speaker change?
			char thisSpeaker = timestampPlusUtterance[0].charAt(timestampPlusUtterance[0].length() - 1); 
			if (thisSpeaker != currSpeaker) {
				if (currSentence.length() > 0) {
					result.add(currSentence.trim());
					currSpeaker = thisSpeaker;
					currSentence = "";
				} else {
					// Initial guess of speaker was wrong 
					currSpeaker = thisSpeaker;
				}
			}
			currSentence += timestampPlusUtterance[1]; 
		}
		// Capture the closing sentence:
		if (currSentence.length() != 0) {
			result.add(currSentence.trim());
		}
		return result;
	}
	
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
