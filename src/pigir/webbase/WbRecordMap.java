/**
 * 
 */
package pigir.webbase;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author paepcke
 * A container for the parts of a WebBase record: WebBase header fields,
 * total record length, and content. The interface adds methods
 * for retrieving just the header portions.  
 *
 */
@SuppressWarnings("hiding")
public interface WbRecordMap<String, V> extends Map<String, V> {
	
	// Keys concerned just with the WebBase header
	// and HTTP header bag (i.e. not content):
	public Set<String> keySetHeader();
	
	// Values with the WebBase header
	// and HTTP header bag (i.e. not page content):
	
	public Collection<Object> valuesHeader();
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
