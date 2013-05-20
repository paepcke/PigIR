package edu.stanford.pigir.fishercollection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class TimestampStripper {

	public TimestampStripper(String directoryRoot, String targetDir) throws IOException {
		this(directoryRoot, targetDir, 0);
	}
	
	public TimestampStripper(String directoryRoot, String targetDir, int outfileSerialNumStart) throws IOException {

		File dirRoot = new File(directoryRoot);
		if (! dirRoot.isDirectory() || ! dirRoot.canWrite()) {
			throw new IOException(String.format("Path '%s' is not an existing, writeable directory.", directoryRoot));
		}

		String[] extensions = new String[] {"txt"};
		
		//*************
		//Collection<File> fileList = FileUtils.listFiles(dirRoot, extensions, true);
		//*************		

		Iterator<File> fileIter = FileUtils.iterateFiles(dirRoot, extensions, true);
		
		while (fileIter.hasNext()) {
			File infile = fileIter.next();
			List<String> allUtterances = FileUtils.readLines(infile);
			Collection<String> flatFileLines =  stripTimestampsAndFlatten(allUtterances);
			// Make an output path:
			String outfileName = infile.getName() + "_" + Integer.toString(outfileSerialNumStart);
			File outfile = new File(targetDir, outfileName);
			FileUtils.writeLines(outfile, flatFileLines);
		}
	}
	
	private Collection<String> stripTimestampsAndFlatten(List<String> allUtterances) {
		ArrayList<String> result = new ArrayList<String>();
		for (String oneLine : allUtterances) {
			String[] timestampPlusUtterance = oneLine.split(":");
			if (timestampPlusUtterance.length < 2)
				continue;
			result.add(timestampPlusUtterance[1]);
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
