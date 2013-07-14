package edu.stanford.pigir.irclientserver;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Server;

public class IRPacket {

	public static class ServiceRequestPacket {
		public String msg;
	};

	public static class ServiceResponsePacket {
		public String msg;
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
		//kryo.register(byte[].class);  // if you want to pass byte arrays, etc.
	}

	public static void registerClasses(Client kryoClient) {
		// Will register all classes that go back and forth
		// between client and server with a serializer.
		// Get that serializer:
		Kryo kryo = kryoClient.getKryo();
		kryo.register(ServiceRequestPacket.class);
		kryo.register(ServiceResponsePacket.class);
		//kryo.register(byte[].class);  // if you want to pass byte arrays, etc.
	}
}
