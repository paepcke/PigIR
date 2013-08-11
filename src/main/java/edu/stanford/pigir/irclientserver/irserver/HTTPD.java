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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
	private String contentLenRegex = "[\\s]*Content-Length[^\\d]*([\\d]+)";
	private Pattern contentLenPattern = Pattern.compile(contentLenRegex, Pattern.CASE_INSENSITIVE);
	
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
				HTTPD.log.error("IOException trying to start a request listener: " + ioe.getMessage());
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
					HTTPD.log.error("IOException while trying to listen to request port: " + ioe.getMessage());
					System.exit(1);
				} catch (NullPointerException e) {
					HTTPD.log.warn("Accept attempt interrupted by null pointer exception; trying again.");
					continue;
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
							String jsonStr = "";
							// Get content length, and place
							// buffer cursor past end of HTTP header:
							int contentLen = readHTTPHeader(br);
							char[] jsonBytes = new char[contentLen];
							int bytesRead    = br.read(jsonBytes, 0, contentLen);
							if (bytesRead != contentLen) {
								HTTPD.log.warn(String.format("HTTP request wrong content length (%d); actual len: %d", contentLen, bytesRead));
								returnHTTPError(bw,**** COMPLETE THIS. Then test TestIRClient again. )
							}
/*							String nxtLine = null;
							while (br.ready()) {
								nxtLine = br.readLine();
								jsonStr += nxtLine;
								nxtLine = br.readLine();
							}
*/							if (jsonStr.length() != contentLen) {
								HTTPD.log.warn(String.format("HTTP request wrong content length (%d) for %s", contentLen, jsonStr));  
							}
							reqPacket = ServiceRequestPacket.fromJSON(jsonStr);
							reqResult = postReqServer.newPigServiceRequest(reqPacket);
						} catch (Exception e) {
							returnHTTPError(bw, 500, "Internal Server Error", e.getMessage() + " (cause: " + e.getClass().getCanonicalName() + ")");
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
					if (mySocket != null)
						mySocket.close();
				} catch(Exception ioe) {
					HTTPD.log.error("IOException while trying to close socket: " + ioe.getMessage());
				}
			}
		}
	}
	
	/**
	 * Given a reader to an HTTP stream at the start of
	 * the HTTP header, return the content length, or 
	 * -1 if no content-length header field is found.
	 * Leave the reader cursor just after the header-delimiting
	 * empty line.
	 * @param reader Reader from which HTTP header is read.
	 * @return content length or -1, if not found.
	 */
	private int readHTTPHeader(BufferedReader reader) {
		String nxtLine = null;
		int contentLength = -1;
		try {
			while (reader.ready()) {
				nxtLine = reader.readLine();
				if ((nxtLine == null) || (nxtLine.length() == 0))
					return contentLength;
				if (contentLength > -1)
					// We already found the content len, just
					// keep eating header lines:
					continue;
				Matcher contentLenMatcher = contentLenPattern.matcher(nxtLine);
				if (contentLenMatcher.find()) {
					String contentLenStr = contentLenMatcher.group(1);
					try {
						contentLength = Integer.parseInt(contentLenStr);
					} catch (NumberFormatException e) {
						HTTPD.log.error(String.format("Content length in HTTP header line %s was not an integer: %s", nxtLine, e.getMessage()));
					}
					continue; // read and discard rest of HTTP header
				}
			}
		} catch (IOException e) {
				HTTPD.log.error("IOError while reading HTTP header: " + e.getMessage());
				return contentLength;
		}
		return contentLength;
	}
	
	private void returnHTTPError(BufferedWriter bw, int HTTPErrorCode, String shortMsg, String bodyMsg) throws IOException {
		bw.write(String.format("HTTP/1.0 %d %s", HTTPErrorCode, shortMsg));
		bw.newLine();
		bw.newLine();
		if (bodyMsg != null)
			bw.write(bodyMsg);
		else
			bw.write(shortMsg);
		bw.flush();
		HTTPD.log.error(String.format("Error during IR request handling: Code %d (%s); %s", HTTPErrorCode, shortMsg, bodyMsg));
	}
}
