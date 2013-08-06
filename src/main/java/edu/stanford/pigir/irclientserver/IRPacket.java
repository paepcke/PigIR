package edu.stanford.pigir.irclientserver;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONString;
import org.codehaus.jettison.json.JSONStringer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
//import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryo.kryonet.Server;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

public class IRPacket {

/*	public static class ServiceRequestPacket {
		public String msg;
	};
*/
	
	public static class ServiceRequestPacket {
		public ClientSideReqID_I clientSideReqId;
		public String operator;
		public Map<String,String> params;
		
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
		
		public JSONStringer toJSON(JSONStringer stringer) {
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
		}
	};
	
	public static class ServiceResponsePacket {
		public ClientSideReqID_I clientSideReqId;
		public JobHandle_I resultHandle;
		
		public JSONStringer toJSON(JSONStringer stringer) {
			stringer.object();
			clientSideReqId.toJSON(stringer);
			stringer.endObject();
			stringer.object();
			resultHandle.toJSON(stringer);
			stringer.endObject();
			
			return stringer;
		}
	};

	/**
	 * Tells kryo about the classes of any objects that will
	 * be sent from clients.
	 * NOTE: on the client, this registration must occur
	 *       as well, and in the same order.
	 */
	public static void registerClasses(Server kryoServer) {
		// Will register all classes that go back and forth
		// between client and server with a serializer.
		// Get that serializer:
		Kryo kryo = kryoServer.getKryo();
		kryo.register(ServiceRequestPacket.class);
		kryo.register(ServiceResponsePacket.class);
		kryo.register(HashMap.class);
		kryo.register(PigServiceHandle.class);
		kryo.register(JobStatus.class);
		kryo.register(ArcspreadException.class);
		kryo.register(ClientSideReqID_I.class);
		kryo.register(ClientSideReqID.class);
		kryo.register(Disposition.class);
		//kryo.register(byte[].class);  // if you want to pass byte arrays, etc.
	}

	public static void registerClasses(Client kryoClient) {
		// Will register all classes that go back and forth
		// between client and server with a serializer.
		// Get that serializer:
		Kryo kryo = kryoClient.getKryo();
		kryo.register(ServiceRequestPacket.class);
		kryo.register(ServiceResponsePacket.class);
		kryo.register(HashMap.class);
		kryo.register(PigServiceHandle.class);
		kryo.register(JobStatus.class);
		kryo.register(ArcspreadException.class);
		kryo.register(ClientSideReqID_I.class);
		kryo.register(ClientSideReqID.class);
		kryo.register(Disposition.class);
		//kryo.register(byte[].class);  // if you want to pass byte arrays, etc.
	}
}
