package edu.stanford.pigir.irclientserver.irclient;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;

public class NetworkListenerClient extends Listener {
	
	private Client kryoClient;
	
	
	public void init(Client client) {
		this.kryoClient = client;
	}
	
	public void connected(Connection conn) {
		Log.info("[Client] Connection to server successful.");
		ServiceRequestPacket req = new ServiceRequestPacket();
		req.msg = "I want service.";
		kryoClient.sendTCP(req);
	}

	public void disconnected(Connection conn) {
		Log.info("[Client] Connectin to IRServer is gone.");		
	}
	
	public void received(Connection conn, Object content) {
		String msg = (String) content;
		System.out.println("[Client] " + msg);
	}
}
