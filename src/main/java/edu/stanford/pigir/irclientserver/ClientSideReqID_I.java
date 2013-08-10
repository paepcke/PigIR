/**
 * 
 */
package edu.stanford.pigir.irclientserver;

import java.net.URI;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;


/**
 * @author paepcke
 * 
 * Used to identify the originator or origination purpose
 * of a client side request source. Instances are passed
 * to IRClient.sendProcessRequest(), and are then automatically
 * associated with a returned-responses queue. ClientSideReqID_I
 * with the same string in the 'id' field all share an output queue.
 * Example: all requests originating from one spreadsheet cell C4
 * might use "C4" as the id. Then all request responses from that
 * cell will accumulate in the "C4" queue.
 *
 */
public interface ClientSideReqID_I {
	
	
	public enum Disposition {
		QUEUE_RESULTS,
		DISCARD_RESULTS,
		NOTIFY;

		public String toJSONValue() {
			return this.name();
		}
		
		public static Disposition fromJSONValue(String jsonValue) {
			for (Disposition anEnumValue : Disposition.values()) {
				if (anEnumValue.toJSONValue() == jsonValue)
					return anEnumValue;
			}
			return null;
		}
	}

	public URI getResultRecipientURI();
	public String getID();
	public String getRequestClass();
	public Disposition getDisposition();
	public JSONStringer toJSON(JSONStringer stringer) throws JSONException;
}
