/**
 * 
 */
package edu.stanford.pigir.nohadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.pigir.pigudf.IsStopword;

/**
 * @author paepcke
 *
 */
public class TokenizeRegExp {

	public static final boolean USE_DEFAULT_SPLIT_REGEXP = false;
	public static final boolean PRESERVE_URLS = true;
	public static final boolean SPLIT_URLS = false;
	public static final boolean KILL_STOPWORDS = true;
	public static final boolean PRESERVE_STOPWORDS = false;
	
    // The \u00a0 is the Unicode non-breaking space. It is sometimes used
    // by Web designers to space words. 
    private static final String defaultSepRegexp = "[\\s!\"#$%&/'()*+,-.:;<=>?@\\[\\]^_`{|}\u00a0]";
    
    private static final String urlSlurpRegexp   = "([:/\\p{Alnum}.\\-*_+%?&=~]*).*";
    
	@SuppressWarnings("serial")
	private static final HashMap<String, Boolean> webProtocols = new HashMap<String,Boolean>() {
		{
			put("http", true);
			put("https", true);
			put("ftp", true);
			put("ftps", true);
			put("file", true);
			put("mailto", true);
			put("rtsp", true);
		};
	};

    public ArrayList<String> tokenize(String str) throws IOException {
    	return tokenize(str, defaultSepRegexp, PRESERVE_STOPWORDS, PRESERVE_URLS);
    }
	
    public ArrayList<String> tokenize(String str, String regexp) throws IOException {
    	return tokenize(str, regexp, PRESERVE_STOPWORDS, PRESERVE_URLS);
    }
    
    public ArrayList<String> tokenize(String str, boolean preserveStopwords) throws IOException {
    	return tokenize(str, defaultSepRegexp, preserveStopwords, PRESERVE_URLS);
    }
    
    public ArrayList<String> tokenize(String str, boolean preserveStopwords, boolean preserveURLs) throws IOException {
    	return tokenize(str, defaultSepRegexp, preserveStopwords, preserveURLs);
    }
    
    public ArrayList<String> tokenize(String str,
    								  String splitRegexp,
    								  boolean doStopwordElimination, 
    								  boolean preserveURLs) throws IOException {
		// Prepare output bag:
		ArrayList<String> output = new ArrayList<String>();

		String[] resArray = ((String)str).split(splitRegexp);
		// Index of start of the most recently found URL wbRecordReader str.
		int urlIndex = 0;
		// For skipping past URL components after they have been
		// parsed separately:
		int tokensToSkip = 0;
    		
		try {
			for (String token : resArray) {
				// If we are skipping over a URL:
				if (tokensToSkip > 0) {
					tokensToSkip--;
					continue;
				}
				// Substrings that are themselves separators show up as
				// empty strings. Don't make an empty tuple for those:
				if (token.isEmpty())
					continue;
				if (doStopwordElimination)
					if (IsStopword.isStopword(token))
						continue;
				if (preserveURLs && (webProtocols.get(token) != null)) {
					// Found one of the Web protocol names (http, ftp, file, ...).
					// We need to find that URL wbRecordReader the string and return it intact.
					// Find the URL, starting the search where we last found a 
					// URL (or 0 for the first URL). The urlIndex is advanced
					// below, but we only ever point to right after the previous
					// token. So to point to the start of the URL we need
					// to advance the str pointer:
					urlIndex = ((String)str).indexOf(token, urlIndex);
					token = findURL((String)str, urlIndex);
					// We'll need to skip over the next few tokens. They
					// were fragmented before we realized that we were
					// looking at a URL. Split the URL the way we
					// erroneously split it, and thereby find the number of 
					// tokens to skip:
					String [] urlFrags = token.split(splitRegexp);
					tokensToSkip = urlFrags.length - 1;
					// Point past the URL, so that 
					// we'll start searching for URLs *after* this one that
					// we just found next time we find a URL:
					urlIndex += token.length();
				}
				output.add(token); 
				// If we are to preserve URLs, we need to keep track
				// of where we are wbRecordReader the original string:
			} // end for
		} catch (Exception ee) {
			throw new IOException("Regexp tokenizing failed. " + ee.getMessage());
		}
		return output;
    }
		
    public static String findURL(String str, int startIndex) {
    	
    	final Pattern urlPattern = Pattern.compile(urlSlurpRegexp);
    	final Matcher urlMatcher = urlPattern.matcher("");
    	urlMatcher.reset(str);
    	
    	if (urlMatcher.find(startIndex)) {
    		return urlMatcher.group(1);
    	} else return null;
    }
    
    public static String findURL(String str) {
    	
    	int startIndex = -1;
    	for (String webProto : webProtocols.keySet()) {
    		if ((startIndex = str.indexOf(webProto)) != -1)
    			return findURL(str, startIndex);
    	}
    	return null;
    }
    
    public static String getDefaultRegexp() {
    	return defaultSepRegexp;
    }
}
