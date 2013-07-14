package edu.stanford.pigir.irclientserver.irserver;

import java.net.InetSocketAddress;
import java.util.Date;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;

public class NetworkListenerServer extends Listener {
	
	private IRServer irServer = null;
	
	public NetworkListenerServer(IRServer parentIRServer) {
		super();
		irServer = parentIRServer;
	}
	
	public void connected(Connection conn) {
		InetSocketAddress ipAndSocket = conn.getRemoteAddressTCP();
		conn.setName("Connection with " + ipAndSocket);
		Date timeStamp = new Date();
		Log.info("[Server] connection from " + conn.toString() + " at " + timeStamp);
	}

	public void disconnected(Connection conn) {
		InetSocketAddress ipAndSocket = conn.getRemoteAddressTCP();
		Date timeStamp = new Date();
		Log.info("[Server] " + ipAndSocket + " disconnected at " + timeStamp);		
	}

	public void received(Connection conn, Object content) {
		ServiceRequestPacket req = null;
		try {
			req = (ServiceRequestPacket) content;
		} catch (Exception e) {
			// Ignore Kryo framework messages, such as keep-alives.
			return;
		}
		req.logMsg();
		// ServiceResponsePacket resp = new ServiceResponsePacket();
		// resp.msg = "Got it.";
		// conn.sendTCP(resp);
	}
}
