package edu.stanford.pigir.fishercollection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import edu.stanford.pigir.warc.WarcFromTextFileMaker;

/**
 * @author paepcke
 *
 * Create WARC files from Fisher Collection files. Each Fisher file will be
 * one gzipped WARC file with one record. Its record header will be:
  			WARC-RECORD-ID
			CONTENT-LENGTH
			WARC-DATE
			WARC-TYPE
			
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

public class FisherCollectionProcessor {
	
	public boolean testing = false;
	
	protected static String fileToTopicPath = "src/main/resources/Datasets/FisherCollection/fileNameToTopic.csv";
	protected static String topicDescPath = "src/main/resources/Datasets/FisherCollection/topicTable.bsv";
	
	List<String> allOutfiles = new ArrayList<String>();
	
	public FisherCollectionProcessor() throws IOException {
		// For testing indidividual service methods
	}
	
	public FisherCollectionProcessor(String directoryRoot, String targetDir) throws IOException {
		
		WarcFromTextFileMaker warcMaker = new WarcFromTextFileMaker(directoryRoot);
		// Get mapping from file numbers to conversation topic:
		HashMap<String,Topic> topicMap = buildFileNameToTopicMap();
		
		while (warcMaker.hasNext()) {
			File infile = warcMaker.next();
			String infileBasename = FilenameUtils.getBaseName(infile.getAbsolutePath());
			
			// Get the Fisher file number, which is the last five digits
			// in, for example, fe_03_05855.txt: 05855 
			String fileNumber = infileBasename.split("_")[2];
			// Almost all Fisher files have an entry in the topic table, but a few
			// don't; skip those:
			Topic fileTopic = topicMap.get(fileNumber);
			if (fileTopic == null)
				continue;
			
			List<String> allUtterances = FileUtils.readLines(infile);
			Collection<String> flatFileLines =  stripTimestampsAndFlatten(allUtterances);
			// Make an output path:
			String outfileName = infileBasename + ".warc";
			File outfile = new File(targetDir, outfileName);
			
			// Construct a WARC header for this file. First a standard header:
			Collection<String> warcHeader = warcMaker.makeWarcHeader(infile.getName(), flatFileLines);
			// Now add the Fisher collection specific fields that document this file's
			// conversation topic:
			warcHeader.add("Fisher-topic-name: " + fileTopic.topicName);
			warcHeader.add("Fisher-topic-short_desc: " + fileTopic.topicShortDesc);
			warcHeader.add("Fisher-topic-desc: " + fileTopic.topicDesc + "\n");
			
			FileUtils.writeLines(outfile, warcHeader);
			FileUtils.writeLines(outfile, flatFileLines, true);
			
			allOutfiles.add(outfile.getAbsolutePath());
		}
	}
	
	public void gzipAllWarcFiles() throws IOException, InterruptedException {
		if (allOutfiles.isEmpty()) {
			throw new IOException("No WARC files are known. Did the FisherCollectionProcessor initialization fail?");
		}
		gzipAllWarcFiles(allOutfiles);
	}
	
	public List<String> gzipAllWarcFiles(List<String> filePathsToGzip) throws IOException, InterruptedException {

		Runtime run = Runtime.getRuntime() ;
		// For testing:
		List<String> cmds = new ArrayList<String>(); 
		
		if (filePathsToGzip.isEmpty()) {
			throw new IOException("No WARC files are known. Did the FisherCollectionProcessor initialization fail?");
		}
		int numBatchesOf16 = filePathsToGzip.size() / 16;
		int remainderBatch = filePathsToGzip.size() % 16;
		String cmd = null;
		for (int i=0; i< numBatchesOf16; i++) {
			cmd = "gzip ";
			for (int j=0; j<16; j++) {
				cmd += filePathsToGzip.get(16*i + j) + " ";
			}
			if (testing)
				cmds.add(cmd);
			else {
				Process pr = run.exec(cmd) ;
				pr.waitFor() ;					
			}
		}
		if (remainderBatch == 0)
			return cmds;
		cmd = "gzip ";
		for (int k=0; k<remainderBatch; k++) {
			cmd += filePathsToGzip.get(numBatchesOf16 * 16 + k) + " ";
		}
		if (testing) {
			cmds.add(cmd);
			return cmds;
		} else {
					Process pr = run.exec(cmd) ;
					pr.waitFor() ;
					return cmds;
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
	 * Given two files: topicTable.bsv (bar-separated file), which maps Fisher
	 * topic names like ENG24 to topic short, and long names. And the second file
	 * mapping Fisher transcript file names topic names: create a HashMap that
	 * maps file name to topic objects.
	 * topicTable looks like this:
	 *      TopicCode|ShortDesc|Desc
     *      ENG01|Professional Sports on TV.|Do either of you have a favorite TV sport?
     *           ...
     * The filename->topic file looks like this:
     *      CALL_ID,TOPICID
	 *      00001,ENG34
	 *	    00002,ENG34
	 * @throws IOException 
 	 *
	 */
	// Should be private, but then junit is not usable
	//private HashMap<String,Topic>buildFileNameToTopicMap() throws IOException {
	public HashMap<String,Topic>buildFileNameToTopicMap() throws IOException {

		HashMap<String,Topic> fileToTopic = new HashMap<String,Topic>();
		HashMap<String,Topic> topicNameToTopicObj = new HashMap<String,Topic>();
		
		List<String> topicLines = FileUtils.readLines(new File(FisherCollectionProcessor.topicDescPath));
		List<String> fileToTopicLines = FileUtils.readLines(new File(FisherCollectionProcessor.fileToTopicPath));
		
		// Build topicName --> Topic instance map
		// Discard field name line:
		topicLines.remove(0);
		
		// Each line will look like:
		//    ENG01|Professional Sports on TV.|Do either of you have a favorite TV sport?
		Iterator<String> topicIter = topicLines.iterator(); 
		
		while (topicIter.hasNext()) {
			String oneTopicLine = topicIter.next();
			String[] topicFields = oneTopicLine.split("\\|");
			Topic newTopic = new Topic(topicFields[0], topicFields[1], topicFields[2]);
			topicNameToTopicObj.put(newTopic.topicName, newTopic);
		}
		
		// Build the result:
		// Discard the field name header line of the file-->topicName line list: 
		fileToTopicLines.remove(0);
		
		// Each line will look like:
		//    00004,ENG34
		Iterator<String> fileToTopicIter = fileToTopicLines.iterator();
		
		while (fileToTopicIter.hasNext()) {
			String fileToTopicLine = fileToTopicIter.next();
			String[] fileToTopicFields = fileToTopicLine.split(",");
			String fileName   = fileToTopicFields[0];
			String topicName  = fileToTopicFields[1];
			Topic  topic = topicNameToTopicObj.get(topicName);
			fileToTopic.put(fileName, topic);
		}
		
		return fileToTopic;
	}
	
	
	class Topic {
		
		public String topicName = null;
		public String topicShortDesc = null;
		public String topicDesc = null;
		
		public Topic(String theTopicName, String theTopicShortDesc, String theTopicDesc) {
			topicName = theTopicName;
			topicShortDesc = theTopicShortDesc;
			topicDesc = theTopicDesc;
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		final String usage = "Usage: java -jar src/main/resources/fisherCollectionProcessor.jar edu.stanford.pigir.fishercollection.FisherCollectionProcessor dirRoot targetDir"; 
		if (args.length != 2) {
			System.out.println("Number of arguments passed in is " + args.length + ". Should be 2:\n" + usage);
			System.exit(1);
		}
		String directoryRoot = args[0];
		String targetDir     = args[1];
		File dirRootFile 	 = new File(directoryRoot);
		File targetDirFile   = new File(targetDir);
		if (! (dirRootFile.isDirectory() && dirRootFile.canRead())) {
			System.out.println("Argument " + directoryRoot + " is not a readable, existing directory.\n" + usage);
			System.exit(1);
		}
		if (! (targetDirFile.isDirectory() && targetDirFile.canRead())) {
			System.out.println("Argument " + targetDir + " is not a readable, existing directory.\n" + usage);
			System.exit(1);
		}
		new FisherCollectionProcessor(directoryRoot, targetDir);
	}
}
