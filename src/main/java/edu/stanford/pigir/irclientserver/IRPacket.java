package edu.stanford.pigir.irclientserver;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONStringer;

import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

public class IRPacket {

	public static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.IRPacket");	

	/*--------------------------
	 * ServiceRequestPacket
	 * -------------------------*/
	
	public static class ServiceRequestPacket {
		public ClientSideReqID_I clientSideReqId;
		public String operator;
		public Map<String,String> params;
				
		private enum ServiceReqPacketJSONDecodeStates {
			GET_REQUEST_BODY,			
			GET_OPERATOR,
			GET_PARM_MAP,
			EXTRACT_PARMS
		}
		
		public ServiceRequestPacket(String theOperator, Map<String,String> theParams, ClientSideReqID_I theReqID) {
			operator = theOperator;
			params   = theParams;
			clientSideReqId = theReqID;
		}
		
		public ClientSideReqID_I getClientSideReqId() {
			return clientSideReqId;
		}

		public String getOperator() {
			return operator;
		}

		public Map<String, String> getParams() {
			return params;
		}

		public String toString() {
			return "<IRRequest " + getOperator() + " " + hashCode() + ">";
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
			log.info("[Server] " + operation);
		}
		
		public void setParameters(Map<String,String> theParams) {
			params = theParams;
		}
		
		public String toJSON() {
			JSONStringer stringer = new JSONStringer();
			try {
				stringer.object(); // outermost JSON obj
				stringer.key("request");
				
				// Build the client side request ID object: 
				JSONStringer subStringer = new JSONStringer();
				subStringer.object();
				clientSideReqId.toJSON(subStringer);
				subStringer.endObject();
				// The client-side request ID:
				JSONObject jsonClientReqObj = new JSONObject(subStringer.toString()); 
				stringer.value(jsonClientReqObj);
				
				stringer.key("operator");
				stringer.value(operator);
				stringer.key("params");
				
				// Build the parameter object:
				subStringer = new JSONStringer();
				subStringer.object();
				if (params != null) {
					for (String parmKey : params.keySet()) {
						subStringer.key(parmKey);
						subStringer.value(params.get(parmKey));
					}
				}
				subStringer.endObject();
				
				stringer.value(new JSONObject(subStringer.toString()));
				stringer.endObject(); // close outermost JSON obj
			} catch (JSONException e) {
				throw new RuntimeException("Trouble creating JSON serialization: " + e.getMessage());
			}
			return stringer.toString();
		}
		
	public static ServiceRequestPacket fromJSON(String jsonStr) throws JSONException {
		ServiceReqPacketJSONDecodeStates decodeState = null;
		ClientSideReqID clientReqID = null;
		String operator = null;
		Map<String,String> params = new HashMap<String,String>();
		ServiceRequestPacket res = null;		
		
		try {
			decodeState = ServiceReqPacketJSONDecodeStates.GET_REQUEST_BODY;
			JSONObject jObj = new JSONObject(jsonStr);
			// Get the ClientSideReqID portion of the JSON string:
			JSONObject jObjReqID = jObj.getJSONObject("request"); 
			clientReqID = ClientSideReqID.fromJSON(jObjReqID.toString());
			
			decodeState = ServiceReqPacketJSONDecodeStates.GET_OPERATOR;
			// Which admin or Pig script to run?
			operator = jObj.getString("operator");
			
			decodeState = ServiceReqPacketJSONDecodeStates.GET_PARM_MAP;
			// Get the parameters Map:
			JSONObject paramObj = jObj.getJSONObject("params"); //***** check null)
			
			decodeState = ServiceReqPacketJSONDecodeStates.EXTRACT_PARMS;
			@SuppressWarnings("unchecked")
			Iterator<String> paramNames = paramObj.keys();
			while (paramNames.hasNext()) {
				String paramName = paramNames.next();
				params.put(paramName, paramObj.getString(paramName));
			}
			res = new ServiceRequestPacket(operator, params, clientReqID);
			return res;
		} catch (JSONException e) {
			String errMsg = null;
			switch (decodeState) {
			case GET_REQUEST_BODY:
				errMsg = String.format("Cannot decode JSON object ServiceRequestPacket: failed while fetching the packet body from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_OPERATOR:
				errMsg = String.format("Cannot decode JSON object ServiceRequestPacket: failed while fetching the Operator from '%s' (%s)", jsonStr, e.getMessage());
			case GET_PARM_MAP:
				errMsg = String.format("Cannot decode JSON object ServiceRequestPacket: failed while fetching the parameter map from '%s' (%s)", jsonStr,  e.getMessage());
			case EXTRACT_PARMS:
				errMsg = String.format("Cannot decode JSON object ServiceRequestPacket: failed while extracting the parameters from '%s' (%s)", jsonStr,  e.getMessage());
			}
			throw new JSONException(errMsg);
		}
	}
		
	}; // end class ServiceRequestPacket
	
	/*--------------------------
	 * ServiceResponsePacket
	 * -------------------------*/
	
	public static class ServiceResponsePacket {
		public ClientSideReqID_I clientSideReqId;
		public JobHandle_I resultHandle;
		
		private enum ServiceRespPacketJSONDecodeStates {
			GET_REQUEST_ID,
			GET_JOB_HANDLE
		}
		
		public ServiceResponsePacket(ClientSideReqID_I theClientReqID, JobHandle_I theResultHandle) {
			clientSideReqId = theClientReqID;
			resultHandle = theResultHandle;
		}

		public ClientSideReqID_I getClientSideReqId() {
			return clientSideReqId;
		}
		
		public JobHandle_I getJobHandle() {
			return resultHandle;
		}

		public String toString() {
			JobStatus status;
			try {
				status = getJobHandle().getStatus();
			} catch (Exception e) {
				status = JobStatus.UNKNOWN;
			}
			return "<IRResponse " + status + " " + hashCode() + ">";
		}
		
		public String toJSON() {
			JSONStringer stringer = new JSONStringer();
			try {
				stringer.object(); // outermost JSON obj
				stringer.key("request");

				// Build the client side request ID object: 
				JSONStringer subStringer = new JSONStringer();
				subStringer.object();
				clientSideReqId.toJSON(subStringer);
				subStringer.endObject();
				// The client-side request ID:
				JSONObject jsonClientReqObj = new JSONObject(subStringer.toString()); 
				stringer.value(jsonClientReqObj);
				
				stringer.key("serviceHandle");
				
				// Build the job handle JSON:
				subStringer = new JSONStringer();
				subStringer.object();
				resultHandle.toJSON(subStringer);
				subStringer.endObject();
				JSONObject jsonJobHandle = new JSONObject(subStringer.toString());
				stringer.value(jsonJobHandle);
				
				stringer.endObject(); // close outermost JSON obj
			} catch (JSONException e) {
				throw new RuntimeException("Trouble creating JSON serialization: " + e.getMessage());
			}
			return stringer.toString();
		}

	public static ServiceResponsePacket fromJSON(String jsonStr) throws JSONException {
		ServiceRespPacketJSONDecodeStates decodeState = null;
		ClientSideReqID clientReqID = null;
		JobHandle_I serviceHandle = null;
		ServiceResponsePacket res = null;		
		
		try {
			// Get the ClientSideReqID portion of the JSON string:
			decodeState = ServiceRespPacketJSONDecodeStates.GET_REQUEST_ID;
			JSONObject jObj = new JSONObject(jsonStr);
			JSONObject jObjReqID = jObj.getJSONObject("request"); 
			clientReqID = ClientSideReqID.fromJSON(jObjReqID.toString());
			
			// Get the job handle:
			decodeState = ServiceRespPacketJSONDecodeStates.GET_JOB_HANDLE;
			JSONObject jObjServiceHandle = jObj.getJSONObject("serviceHandle");
			serviceHandle = PigServiceHandle.fromJSON(jObjServiceHandle.toString());
			
			res = new ServiceResponsePacket(clientReqID, serviceHandle);
			return res;
		} catch (JSONException e) {
			String errMsg = null;
			switch (decodeState) {
			case GET_REQUEST_ID:
				errMsg = String.format("Cannot decode JSON object ServiceResponsePacket: failed while fetching the request ID from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_JOB_HANDLE:
				errMsg = String.format("Cannot decode JSON object ServiceResponsePacket: failed while fetching the job handle from '%s' (%s)", jsonStr, e.getMessage());
			}
			throw new JSONException(errMsg);
		}
	}
	} // end class ServiceResponsePacket
}
