package edu.stanford.pigir.irclientserver.irserver;

import java.io.IOException;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.PigService;
import edu.stanford.pigir.irclientserver.hadoop.PigScriptRunner;

public class IRServer {
	
		
	public static int IRSERVICE_PORT = 4040;
	
    // IRServer is a singleton:
	private static IRServer theInstance = null;

	private Server kryoServer;
	
	/**
	 * Get the singleton instance of the IRServer, creating
	 * one if one exists. As a side effect, this call refreshes
	 * the server's knowledge of which Pig scripts are available.
	 * So, if a new Pig script (a.k.a. operator) is added, a 
	 * call to getInstance() will make that new script known even
	 * to a running server.
	 * @return The singleton IRServer
	 * @throws IOException
	 */
	public static IRServer getInstance() throws IOException {
		if (theInstance != null) {
			 return theInstance;
		}
		IRServer.theInstance = new IRServer();
		return IRServer.theInstance;
	}
	
	private IRServer() throws IOException {
		kryoServer = new Server();
		IRPacket.registerClasses(kryoServer);
		// Get a network port listener:  
		kryoServer.addListener(new NetworkListenerServer(this));
		// Bind server to TCP (only, not to UDP as well):
		kryoServer.bind(IRSERVICE_PORT); 
		kryoServer.start();
	}
	
	public void serviceHadoopRequest(Connection kryoConn, ServiceRequestPacket requestPacket) {
		PigService pigService = new PigScriptRunner();
		pigService.servicePigRequest(requestPacket.operator, requestPacket.params);
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
		Log.info("IR Server running at " + IRSERVICE_PORT);
	}

}
