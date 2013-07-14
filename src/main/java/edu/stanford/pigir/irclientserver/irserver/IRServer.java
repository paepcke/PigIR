package edu.stanford.pigir.irclientserver.irserver;

import java.io.IOException;

import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket;

public class IRServer {
	
	public static int IRSERVICE_PORT = 4040;
	
	private Server kryoServer;
	
	public IRServer() throws IOException {
		kryoServer = new Server();
		IRPacket.registerClasses(kryoServer);
		// Get a network port listener:  
		kryoServer.addListener(new NetworkListenerServer());
		// Bind server to TCP (only, not to UDP as well):
		kryoServer.bind(IRSERVICE_PORT); 
		kryoServer.start();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Log.set(Log.LEVEL_DEBUG);
		try {
			new IRServer();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Log.info("IR Server running.");
	}

}
