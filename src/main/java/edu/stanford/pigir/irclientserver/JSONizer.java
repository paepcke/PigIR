/**
 * 
 */
package edu.stanford.pigir.irclientserver;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;

/**
 * Generates JSON for requests/responses that need to
 * be passed between PigIR clients and the PigIR server.
 * 
 * @author paepcke
 *
 */
public class JSONizer {
	
	public static String convertServiceRequestPacket(ServiceRequestPacket reqPacket) throws JSONException {
		
		JSONStringer stringer = new JSONStringer();
		stringer.object();
		reqPacket.toJSON(stringer);
		stringer.endObject();
		
		return stringer.toString();
	
	}
	
	public static String convertServiceResponsePacket(ServiceResponsePacket responsePacket) throws JSONException {
		
		JSONStringer stringer = new JSONStringer();
		stringer.object();
		responsePacket.toJSON(stringer);
		stringer.endObject();
		
		return stringer.toString();
	}
}
