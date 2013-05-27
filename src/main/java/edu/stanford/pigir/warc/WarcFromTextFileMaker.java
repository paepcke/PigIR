/**
 * 
 */
package edu.stanford.pigir.warc;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;

/**
 * @author paepcke
 * Completed WarcFromTextFileMaker.java, which provides an iterator over 
 * a directory tree of text files. Clients read one file after the other, 
 * passing each resulting array of file lines back into the WarcFromTextFileMaker 
 * to receive a well formed WARC header. The client can than prepend that header 
 * to its content before writing to an output file.
 * 
 * See, for example, FisherCollectionProcessor.java.
 */
public class WarcFromTextFileMaker {
	
	Iterator<File> fileIter = null;
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

	public WarcFromTextFileMaker(String directoryRoot) throws IOException {
		File dirRoot = new File(directoryRoot);
		if (! dirRoot.isDirectory() || ! dirRoot.canWrite()) {
			throw new IOException(String.format("Path '%s' is not an existing, writeable directory.", directoryRoot));
		}

		String[] extensions = new String[] {"txt"};
		fileIter = FileUtils.iterateFiles(dirRoot, extensions, true);
	}
	
	public boolean hasNext() {
		return fileIter.hasNext();
	}
	
	public File next() {
		return fileIter.next();
	}

	/**
	 * Given a collection of strings, create a WARC header that 
	 * is appropriate for that collection. NOTE: caller is responsible
	 * for adding an empty line between his header and the content.
	 * The empty line is not added, in case the caller wishes to add
	 * additional header fields.
	 * 
	 *       WARC/1.0
	 *		 WARC-Type: resource
	 *		 WARC-Date: 2012-12-14T23:58:24Z
	 *		 WARC-Record-ID: file:///foo/bar/fum.txt
	 *		 Content-Type: application/warc
	 *		 Content-Length: 456
	 *
	 * @param contentLines
	 * @return
	 */
	public Collection<String> makeWarcHeader(String originResourceFile, Collection<String> contentLines) {
		ArrayList<String> res = new ArrayList<String>();
		res.add("WARC/1.0");
		res.add("WARC-Type: resource");
		res.add("WARC-Date: " + df.format(new Date()));
		res.add("WARC-RECORD-ID: file:///" + originResourceFile);
		res.add("Content-Type: application/warc");
		
		// Compute total content length, with single-char newlines,
		// but without the WARC header itself:
		int len = 0;
		for (String line : contentLines) {
			len += line.length() + 1;
		}
		res.add("Content-Length: " + len);
		return res;
	}
}
