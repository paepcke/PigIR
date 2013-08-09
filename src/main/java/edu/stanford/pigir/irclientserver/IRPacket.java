package edu.stanford.pigir.irclientserver;

import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;

import com.esotericsoftware.minlog.Log;

public class IRPacket {

	
	/*--------------------------
	 * ServiceRequestPacket
	 * -------------------------*/
	
	public static class ServiceRequestPacket {
		public ClientSideReqID_I clientSideReqId;
		public String operator;
		public Map<String,String> params;
		
		public ServiceRequestPacket(String theOperator, Map<String,String> theParams, ClientSideReqID_I theReqID) {
			operator = theOperator;
			params   = theParams;
			clientSideReqId = theReqID;
		}
		
		public void logMsg() {
			String operation = operator;
			if (params != null) {
				operation += "(";
				for (String key : params.keySet()) {
					operation += key + "=" + params.get(key) + ",";
				}
				if (operation.endsWith(","))
					operation = operation.substring(0,operation.length()-1);
			}
			operation += ")";
			Log.info("[Server] " + operation);
		}
		
		public void setParameters(Map<String,String> theParams) {
			params = theParams;
		}
		
		public JSONStringer toJSON() {
			JSONStringer stringer = new JSONStringer();
			try {
				stringer.object(); // outermost JSON obj
				stringer.key("request");
				
				// Build the client side request ID object: 
				JSONStringer subStringer = new JSONStringer();
				subStringer.object();
				clientSideReqId.toJSON(subStringer);
				subStringer.endObject();
				
				stringer.value(subStringer.toString());
				
				stringer.key(operator);
				
				// Build the parameter object:
				subStringer = new JSONStringer();
				subStringer.object();
				for (String parmKey : params.keySet()) {
					subStringer.key(parmKey);
					subStringer.value(params.get(parmKey));
				}
				subStringer.endObject();
				
				stringer.value(subStringer.toString());
				stringer.endObject(); // close outermost JSON obj
			} catch (JSONException e) {
				throw new RuntimeException("Trouble creating JSON serialization: " + e.getMessage());
			}
			return stringer;
		}
	};
	
	/*--------------------------
	 * ServiceResponsePacket
	 * -------------------------*/
	
	public static class ServiceResponsePacket {
		public ClientSideReqID_I clientSideReqId;
		public JobHandle_I resultHandle;
		
		public JSONStringer toJSON() {
			JSONStringer stringer = new JSONStringer();
			try {
				stringer.object(); // outermost JSON obj
				stringer.object();
				clientSideReqId.toJSON(stringer);
				stringer.endObject();
				stringer.object();
				resultHandle.toJSON(stringer);
				stringer.endObject();
				stringer.endObject(); // close outermost JSON obj
			} catch (JSONException e) {
				throw new RuntimeException("Trouble creating JSON serialization: " + e.getMessage());
			}
			return stringer;
		}
	};
}
