package edu.stanford.pigir.irclientserver;

import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;

import com.esotericsoftware.minlog.Log;

public class IRPacket {

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
		
		public JSONStringer toJSON() {
			JSONStringer stringer = new JSONStringer();
			try {
				stringer.object(); // outermost JSON obj
				stringer.object();
				clientSideReqId.toJSON(stringer);
				stringer.endObject();
				stringer.key(operator);
				stringer.value(stringer.object());
				for (String parmKey : params.keySet()) {
					stringer.key(parmKey);
					stringer.value(params.get(parmKey));
				}
				stringer.endObject();
				stringer.endObject(); // close outermost JSON obj
			} catch (JSONException e) {
				throw new RuntimeException("Trouble creating JSON serialization: " + e.getMessage());
			}
			return stringer;
		}
	};
	
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
