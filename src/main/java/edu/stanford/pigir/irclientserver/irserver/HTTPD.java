package edu.stanford.pigir.irclientserver.irserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.PigService_I;

public class HTTPD {
	
	PigService_I postReqServer = null;
	private static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.irserver.HTTPD");
	
	public HTTPD (int port, PigService_I thePostReqServer) {
		postReqServer = thePostReqServer;
		ServerSocket serverSock = null;
		HTTPD.log.setLevel(Level.DEBUG);
		BasicConfigurator.configure();
		try {
			serverSock = new ServerSocket(port);
		} catch(IOException ioe) {
			System.err.println("ServerSocket()");
			System.exit(1);
		}
		while (true) {
			Socket newSocket = null;
			try {
				newSocket = serverSock.accept();
			} catch(IOException ioe) {
				System.err.println("accept()");
				System.exit(1);
			}
			new ReqDispatcher(newSocket).start();
		}
	}
	
	private class ReqDispatcher extends Thread {
		private Socket mySocket;

		public ReqDispatcher (Socket requestSocket) {
			mySocket = requestSocket;
		}

		public void run () {
			try {
				InputStream is = mySocket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				String command[] = br.readLine().split("\\s+");
				if (3 == command.length) {
					OutputStream os = mySocket.getOutputStream();
					OutputStreamWriter osw = new OutputStreamWriter(os);
					BufferedWriter bw = new BufferedWriter(osw);
					if ("POST".equals(command[0])) {
						ServiceRequestPacket reqPacket = new ServiceRequestPacket("foo", null, null); // ******
						JobHandle_I reqResult = postReqServer.newPigServiceRequest(reqPacket);
						JSONStringer stringer = new JSONStringer();
						stringer.object();
						stringer = reqResult.toJSON(stringer);
						stringer.endObject();
						String res = stringer.toString();
							bw.write("HTTP/1.0 200 Ok");
							bw.newLine();
							bw.write(String.format("Content-Type: %s", "application/json"));
							bw.newLine();
							bw.write(String.format("Content-Length: %d", res.length()));
							bw.newLine();
							bw.newLine();
							bw.write(res);
							bw.flush();
						}
						else {
							bw.write("HTTP/1.0 404 Not Found");
							bw.newLine();
							bw.newLine();
							bw.flush();
						}
				}

			} catch(IOException ioe) {
				log.error("IOError while handling HTTP request: " + ioe.getMessage());
			} catch (JSONException e) {
				log.error("JSON construction exception while handling HTTP request: " + e.getMessage());
			}
			try {
				mySocket.close();
			} catch(IOException ioe) {
			}
		}
	}
}
