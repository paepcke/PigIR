package edu.stanford.pigir.irclientserver;

import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

public class IRPacket {

/*	public static class ServiceRequestPacket {
		public String msg;
	};
*/
	
	public static class ServiceRequestPacket {
		public String operator;
		public Map<String,String> params;
		public ClientSideReqID_I clientSideReqId;
		
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
	};
	
	public static class ServiceResponsePacket {
		public JobHandle_I resultHandle;
		public ClientSideReqID_I clientSideReqId;
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
