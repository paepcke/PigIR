/**
 * 
 */
package edu.stanford.pigir.irclientserver;

import org.codehaus.jettison.json.JSONStringer;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;

/**
 * Generates JSON for requests/responses that need to
 * be passed between PigIR clients and the PigIR server.
 * 
 * @author paepcke
 *
 */
public class JSONizer {
	
	public static String convertServiceRequestPacket(ServiceRequestPacket reqPacket) {
		//String res = String.format("{operator : %s, ", reqPacket.operator);
		
		JSONStringer res = new JSONStringer();
		res.object();
		res.key("operator");
		res.value(reqPacket.operator);
		
		
		return res;
	
		
		
/*		public String operator;
		public Map<String,String> params;
		public ClientSideReqID_I clientSideReqId;
*/				
	}
}
