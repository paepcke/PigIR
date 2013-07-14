package edu.stanford.arcspreadux.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

public class PigScriptReader implements Iterator<String> {
	
	BufferedReader reader = null;
	
	
	public PigScriptReader(String pathName) throws FileNotFoundException {
		reader = new BufferedReader(new FileReader(pathName));
	}

	public PigScriptReader(File scriptFile) throws FileNotFoundException {
		this(scriptFile.getAbsolutePath());
	}
	
	public PigScriptReader(InputStream scriptStream) throws FileNotFoundException {
		reader = new BufferedReader(new InputStreamReader(scriptStream));
	}
	
	public boolean hasNext() {
		try {
			return reader.ready();
		} catch (IOException e) {
			return false;
		}
	}

	public String next() {
		String query = "";
		boolean atQueryEnd = false;
		while (! atQueryEnd) {
			try {
				String line = reader.readLine();
				// Enf of file?
				if (line == null) {
					atQueryEnd = true;
					continue;
				}
				
				// An inline comment anywhere?
				line = stripInlineComment(line);
				
				// A block comment start anywhere?
				int commentStart = line.indexOf("/*"); 
				if (commentStart != -1) {
					query += line.substring(0,commentStart);
					// Find end of comment:
					line = skipBlockComment(line.substring(commentStart));
				}
				
				
				// Append (possibly partial) code to growing query str:
				query += " " + line.trim();
				
				// Found end of query line? 
				int statementEndPos = line.indexOf(';');
				if (statementEndPos != -1) {
					atQueryEnd = true;
					continue;
				}
			} catch (IOException e) {
				atQueryEnd = true;
			}
		}
		return query.trim();
	}

	private String stripInlineComment(String line) {
		int commentStart = line.indexOf("--"); 
		if (commentStart != -1) {
			return line.substring(0,commentStart);
		}
		else {
			return line;
		}
	}
	
	private String skipBlockComment(String firstCommentLine) throws IOException {
		int endOfComment = firstCommentLine.indexOf("*/");
		if (endOfComment != -1) {
			return firstCommentLine.substring(endOfComment + 2);
		}
		while (true) {
			String commentLine = reader.readLine();
			int commentEnd = commentLine.indexOf("*/");
			if (commentEnd == -1) {
				continue;
			}
			return(stripInlineComment(commentLine.substring(commentEnd + 2)));
		}
	}
	
	public void remove() {
		throw new NotImplementedException();
	}
}
