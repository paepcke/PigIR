/**
 * 
 */
package edu.stanford.pigir.warc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author paepcke
 *
 * Instances of WarcFilter can test whether a given WarcRecord
 * instance contains a given key, and the corresponding value
 * satisfies a given regular expression. The class is intended
 * to be instantiated once, and the resulting instance used 
 * many times as a stream of WARC records is examined. Main
 * methods at contentsIf(), contentsIfNot(), allIf(), and allIfNot().
 * The contentsXXX() methods return only the content parts of
 * the record, without the WARC header, while the allXXX() methods'
 * returns include the WARC headers.
 * 
 * Currently only values for a single key can be tested. The
 * absence of the key in a given WARC record is considered a
 * non-match (rather than an error).
 * 
 * WARC record keys are:
 *     warc-type
 *     warc-record-id
 *     warc-date
 *     content-length
 *     content-type
 *     warc-concurrent-To
 *     warc-block-digest
 *     warc-payload-digest
 *     warc-ip-address
 *     warc-refers-to
 *     warc-target-uri
 *     warc-truncated
 *     warc-warcinfo-id
 *     warc-filename
 *     warc-profile
 *     warc-identified-payload-type
 *     warc-segment-origin-id
 *     warc-segment-number
 *     warc-segment-total-length
 * 
 * Only content-length, warc-date, and warc-type are mandatory for WARC records.
 */
public class WarcFilter {
	
	private Pattern regexPattern = null;
	private String warcFieldKey = null;

	public WarcFilter(String warcValRegexPatternStr, String warcRecKey) {
		regexPattern = Pattern.compile(warcValRegexPatternStr);
		warcFieldKey = warcRecKey;
	}
	
	/**
	 * Given a WARC record, determine whether the filter's key
	 * is associated with a value that matches this filter's regular
	 * expression.
	 * @param warcRec record to test
	 * @return whether the record's value for the this filter's key matches this filter's regular expression
	 */
	public boolean matches(PigWarcRecord warcRec) {
		String val = warcRec.get(warcFieldKey);
		if (val == null)
			return false;
		Matcher m = regexPattern.matcher(val);
		//boolean matches = m.matches();
		return m.matches();
	}
	
	/**
	 * Returns the given WARC record's content without any metadata,
	 * if the record matches the filter. Else returns null.
	 * @param warcRec
	 * @return
	 */
	public String contentsIf(PigWarcRecord warcRec) {
		if (matches(warcRec))
			// Return content:
			return warcRec.get("content");
		return null;
	}
	
	/**
	 * Returns the given WARC record's content without any metadata,
	 * if the record does NOT match the filter. Else returns null. This
	 * is a convenience method so callers do not have to deal with 
	 * negative lookahead regex patterns.
	 * @param warcRec
	 * @return
	 */
	public String contentsIfNot(PigWarcRecord warcRec) {
		if (!matches(warcRec))
			// Return content:
			return warcRec.get("content");
		return null;
	}

	
	/**
	 * Returns the given WARC record's content, including the WARC header
	 * metadata, if the filter matches. Else returns null.
	 * @param warcRec
	 * @return
	 */
	public String allIf(PigWarcRecord warcRec) {
		if (matches(warcRec))
			return warcRec.toString(PigWarcRecord.INCLUDE_CONTENT);
		return null;
	}

	/**
	 * Returns the given WARC record's content, including the WARC header
	 * metadata, if the filter does NOT match. Else returns null. This 
	 * is a convenience method so callers do not have to deal with 
	 * negative lookahead regex patterns.
	 * @param warcRec
	 * @return
	 */
	public String allIfNot(PigWarcRecord warcRec) {
		if (!matches(warcRec))
			return warcRec.toString(PigWarcRecord.INCLUDE_CONTENT);
		return null;
	}

}
