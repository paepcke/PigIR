package edu.stanford.pigir.irclientserver.irserver;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;

public class NetworkListenerServer extends Listener {
	
	private Client kryoClient;
	
	
	public void init(Client client) {
		this.kryoClient = client;
	}
	
	public void connected(Connection conn) {
		Log.info("[Server] Someone connected.");
	}

	public void disconnected(Connection conn) {
		Log.info("[Server] Someone disconnected.");		
	}
	
	public void received(Connection conn, Object content) {
		ServiceRequestPacket req = (ServiceRequestPacket) content;
		Log.info("[Server] " + req.msg);
		ServiceResponsePacket resp = new ServiceResponsePacket();
		resp.msg = "Got it.";
		conn.sendTCP(resp);
	}
	
}
