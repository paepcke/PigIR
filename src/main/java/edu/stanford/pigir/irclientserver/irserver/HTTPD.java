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

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.PigService_I;

/**
 * @author paepcke. Based on lu1s/http-java-server/Httpd.java at Github 
 *
 *
 */
public class HTTPD {
	
	PigService_I postReqServer = null;
	private static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.irserver.HTTPD");
	
	public HTTPD (int port, PigService_I thePostReqServer) {
		postReqServer = thePostReqServer;
		HTTPD.log.setLevel(Level.DEBUG);
		BasicConfigurator.configure();
		RequestListener listenerThread = new RequestListener().start(port);
		HTTPD.log.info("IR Server running at " + port);
		try {
			listenerThread.join();
		} catch (InterruptedException e) {
			// TODO Maybe needs to re-join on interrupt?
			e.printStackTrace();
		}
	}
	
	/**
	 * Thread for listening to incoming requests: 
	 * @author paepcke
	 *
	 */
	private class RequestListener extends Thread {
		
		ServerSocket serverSock = null;
		
		public RequestListener start(int port) {
			super.start();
			try {
				serverSock = new ServerSocket(port);
			} catch(IOException ioe) {
				System.err.println("ServerSocket()");
				System.exit(1);
			}
			return this;
		}
		
		
		public void run() {
			while (true) {
				Socket newSocket = null;
				try {
					newSocket = serverSock.accept();
				} catch(IOException ioe) {
					System.err.println("accept()");
					System.exit(1);
				}
				new ReqHandler(newSocket).start();
			}
		}
	}
	
	
	/**
	 * Thread for processing one incoming request 
	 * @author paepcke
	 *
	 */
	private class ReqHandler extends Thread {
		private Socket mySocket;

		public ReqHandler (Socket requestSocket) {
			mySocket = requestSocket;
		}

		public void run () {
			try {
				InputStream is = mySocket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				
				OutputStream os = mySocket.getOutputStream();
				OutputStreamWriter osw = new OutputStreamWriter(os);
				BufferedWriter bw = new BufferedWriter(osw);
				ServiceResponsePacket reqResult = null;

				String command[] = br.readLine().split("\\s+");
				if (3 == command.length) {
					if ("POST".equals(command[0])) {
						ServiceRequestPacket reqPacket = null;
						try {
						reqPacket = new ServiceRequestPacket("foo", null, null); // ******
						reqResult = postReqServer.newPigServiceRequest(reqPacket);
						} catch (Exception e) {
							returnHTTPError(bw, 500, "Internal Server Error", e.getMessage());
							return;
						}
						String jsonResp = reqResult.toJSON();
						bw.write("HTTP/1.0 200 Ok");
						bw.newLine();
						bw.write(String.format("Content-Type: %s", "application/json"));
						bw.newLine();
						bw.write(String.format("Content-Length: %d", jsonResp.length()));
						bw.newLine();
						bw.newLine();
						bw.write(jsonResp);
						bw.flush();
					}
					else {
						returnHTTPError(bw, 400," Bad Request", "Only POST messages are used in IR Service.");
					}
				}

			} catch(IOException ioe) {
				log.error("IOError while handling HTTP request: " + ioe.getMessage());
			} finally {
				try {
					mySocket.close();
				} catch(Exception ioe) {
				}
			}
		}
	}
	
	private void returnHTTPError(BufferedWriter bw, int HTTPErrorCode, String shortMsg, String bodyMsg) throws IOException {
		bw.write(String.format("HTTP/1.0 %d %s", HTTPErrorCode, shortMsg));
		bw.newLine();
		bw.newLine();
		bw.write(bodyMsg);
		bw.flush();
		HTTPD.log.error(String.format("Error during IR request handling: Code %d (%s); %s", HTTPErrorCode, shortMsg, bodyMsg));
	}
}
