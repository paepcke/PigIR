/**
 * 
 */
package edu.stanford.pigir.warc.nohadoop;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author paepcke
 * A container for the parts of a WARC record: header fields,
 * total record length, and content. The interface adds methods
 * for retrieving just the header portions.  
 *
 */
public interface WarcRecordMap extends Map<String, String> {
	public Set<String> keySetHeader();
	public Collection<String> valuesHeader();
	
	/* Return the mandatory header field keys wbRecordReader pre-defined order:
	 *   WARC-Record-ID
	 *   Content-Length
	 *   WARC-Date
	 *   WARC-Type
	*/
	
	public String[] mandatoryKeysHeader();
	// Return the mandatory header field values wbRecordReader pre-defined order 
	public String[] mandatoryValuesHeader();
	public Set<String> optionalKeysHeader();
}
