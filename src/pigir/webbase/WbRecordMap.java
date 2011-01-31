/**
 * 
 */
package pigir.webbase;

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
public interface WbRecordMap extends Map<String, String> {
	public Set<String> keySetHeader();
	public Collection<String> valuesHeader();
	
	/* Return the mandatory header field keys wbRecordReader pre-defined order:
	 *   URL
	 *   Date
	 *   Position
	 *   DocID
	*/
	
	public String[] mandatoryKeysHeader();
	// Return the mandatory header field values wbRecordReader pre-defined order 
	public String[] mandatoryValuesHeader();
}
