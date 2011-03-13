package pigir;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import pigir.webbase.Constants;
import pigir.webbase.WbRecordReader;

public final class Common {
	
	public static final String PIG_TRUE = "1";
	public static final String PIG_FALSE = "null";
	
	public static void print(PigServer pserver, String token) {
		Iterator<Tuple> res;
		try {
			res = pserver.openIterator(token);
			while (res.hasNext()) {
				System.out.println(res.next());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*-----------------------------------------------------
	| print()
	------------------------*/
	/**
	 * Print all elements of a tuple
	 * @param pserver Pig server
	 * @param tuple The tuple to print
	 */
	public static void print(PigServer pserver, Tuple tuple) {
		for (Object tupleEl : tuple.getAll()) {
			System.out.println(tupleEl);
		}
	}
	
	/**
	 * Print limit elements of a tuple 
	 * @param pserver Pig server
	 * @param tuple The tuple to print
	 * @param limit Number of elements to print from the tuple
	 */
	public static void print(PigServer pserver, Tuple tuple, int limit) {
		for (int i=0; i<limit; i++) {
			try {
				System.out.println(tuple.get(i));
			} catch (ExecException e) {
				e.printStackTrace();
			}
		}
	}
	
	/*-----------------------------------------------------
	| openTmpFile() 
	------------------------*/
	
	/**
	 * Create and open a tmp file on the Pig server machine this process is running on.
	 * @param pserver The Pig server
	 * @return An open temporary file
	 * @throws IOException
	 */
	public static File openTmpFile(PigServer pserver) throws IOException {
		
			String tmpDir = pserver.getPigContext().getProperties().getProperty("mapred.child.tmp");
			String userName = System.getProperty("user.name"); 
			if (tmpDir == null || tmpDir.isEmpty())
				tmpDir = "/tmp/" + userName;
			return File.createTempFile(userName, "tmp", new File(tmpDir));
	}
	
	/*-----------------------------------------------------
	| getSocket()
	------------------------*/
	
	/**
	 * Open a socket. We have all socket opening go through this method
	 * so that we can keep track. Callers have the option of specifying
	 * multiple attempts.
	 *  
	 * @param machineName Name of target machine 
	 * @param port Port to connect to at target machine
	 * @param numAttempts Number of times a connection attempt is to be made
	 * @param infoOnError String to use in error messages. Messages generated with this method
	 * will automatically add the machine name and port in parentheses. So this information is not
	 * required in this parameter.
	 * @return An open socket.
	 * @throws IOException
	 */
	public static Socket getSocket(String machineName, int port, int numAttempts, String infoOnError) throws IOException {
		
		int attempts = numAttempts;
		Socket resSock = null;
		Logger logger = WbRecordReader.getLogger();;
		// Index into array of wait-between-attempts times (DISTRIB_DEMON_ATTEMPT_PAUSES):
		int timeoutIndex = 0;
		
		while ((--attempts >= 0) && (resSock == null) || !resSock.isConnected()) {
			// Set up socket and input/output streams
			try {
				resSock = new Socket(machineName, port);
			} catch (UnknownHostException e) {
				String errMsg = infoOnError + "(" + machineName + ":" + port + "). Unknown host.";
				logger.error(errMsg);
				throw new IOException(errMsg);
			} catch (IOException e) {
				if (attempts > 0) {
					try {
						logger.warn("Socket open failed: " + infoOnError + "(" + machineName + ":" + port + "). Retrying...");
						Thread.sleep(1000 * Constants.SOCKET_OPEN_ATTEMPT_PAUSES[timeoutIndex++]);
					} catch (InterruptedException e1) {
						// ignore
					}
					// Try again:
					continue;
				}
				String errMsg = "Failed to open socket. " + infoOnError + "(" + machineName + ":" + port + "). " + e.getMessage();
				logger.error(errMsg);
				throw new IOException(errMsg);
			}
		}
		return resSock;
	}
	
	/**
	 * @see getSocket(String machineName, int port, int numAttempts, String infoOnError)
	 * @param iPAddr IP address object of target machine.
	 * @param port port Port to connect to at target machine
	 * @param numAttempts numAttempts Number of times a connection attempt is to be made
	 * @param infoOnError infoOnError String to use in error messages. Messages generated with this method
	 * will automatically add the machine name and port in parentheses. So this information is not
	 * required in this parameter.
	 * @return
	 * @throws IOException
	 */
	public static Socket getSocket(InetAddress iPAddr, int port, int numAttempts, String infoOnError) throws IOException {
		return getSocket(iPAddr.getHostAddress(), port);
	}
	
	/**
	 * @param machineName
	 * @param port
	 * @param infoOnError
	 * @return
	 * @throws IOException
	 */
	public static Socket getSocket(String machineName, int port, String infoOnError) throws IOException {
		return getSocket(machineName, port, 1, infoOnError); // only one opening  attempt
	}
	/**
	 * @param iPAddr
	 * @param port
	 * @param infoOnError
	 * @return
	 * @throws IOException
	 */
	public static Socket getSocket(InetAddress iPAddr, int port, String infoOnError) throws IOException {
		return getSocket(iPAddr.getHostAddress(), port, 1, infoOnError); // only one opening  attempt
	}
	
	/**
	 * @param machineName
	 * @param port
	 * @return
	 * @throws IOException
	 */
	public static Socket getSocket(String machineName, int port) throws IOException {
		return getSocket(machineName, port, 1, ""); // only one attempt, no error message clarification.
	}
	/**
	 * @param iPAddr
	 * @param port
	 * @return
	 * @throws IOException
	 */
	public static Socket getSocket(InetAddress iPAddr, int port) throws IOException {
		return getSocket(iPAddr.getHostAddress(), port, 1, ""); // only one attempt, no error message clarification.
	}
	
	
	/*-----------------------------------------------------
	| getInetAddressFromAsciiIP()
	------------------------*/
	
	/**
	 * Given an ASCII format IP address string, return an InetAddress object.
	 * @param ip
	 * @return
	 * @throws UnknownHostException
	 */
	public static InetAddress getInetAddressFromAsciiIP(String ip) throws UnknownHostException {
		String[] ipComponents = ip.split("\\.");
		byte[] ipBytes = {(byte) Integer.parseInt(ipComponents[0]), 
				          (byte) Integer.parseInt(ipComponents[1]),
				          (byte) Integer.parseInt(ipComponents[2]),
				          (byte) Integer.parseInt(ipComponents[3])
				                                          
				};
		return InetAddress.getByAddress(ipBytes);
	}
	
}
