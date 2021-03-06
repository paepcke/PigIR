package edu.stanford.pigir;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.JarURLConnection;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

import edu.stanford.pigir.webbase.Constants;
import edu.stanford.pigir.webbase.WbRecordReader;

public final class Common {
	
	public static final String PIG_TRUE = "1";
	public static final String PIG_FALSE = "null";
	
	/*-----------------------------------------------------
	| print()
	------------------------*/
	
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
	
	public static void print(PigServer pserver, String token, File outFile) {
		Iterator<Tuple> res;
		try {
			res = pserver.openIterator(token);
			while (res.hasNext()) {
				// Append next tuple to file:
				FileUtils.write(outFile, res.next().toString(), true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
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
	
	public static void print(PigServer pserver, DataBag bag) {
		Iterator<Tuple> bagIterator = bag.iterator();
		while (bagIterator.hasNext()) {
			System.out.println(bagIterator.next());
		}
	}

	/*-----------------------------------------------------
	| getTupleSchema()
	------------------------*/
	
	public static Schema getTupleSchema(Tuple theTuple) {
		Schema resSchema = new Schema();
		for (int i=0; i<theTuple.size(); i++) {
			try {
				resSchema.add(new Schema.FieldSchema("field" + i, theTuple.getType(i)));
			} catch (ExecException e) {
				// This error is thrown for out-of-bound index. 
				// We watch for that above:
			}
		}
		return resSchema;
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
	| fileViaClasspath() 
	------------------------*/
	
	/**
	 * Given a resource that is in the system classpath, return a File
	 * that contains the full pathname. Example1:
	 * 
	 * 		Common.fileViaClasspath("edu/stanford/pigir/Common.class")
	 * 
	 * returns the full path to this Common class, which is
	 * <mavenRootDir>/target/classes/edu/stanford/pigir/Common.class.
	 * Example2:
	 * 
	 * 		Common.fileViaClasspath("warcStripHTML.pig");
	 * 
	 * returns the path to <mavenRootDir>/target/classes/warcStripHTML.pig
	 * Note that the resource path mirrors the package hierarchy, you
	 * use '/' in the parameter string.
	 * 
	 * @param fileBaseName is the package path
	 * @return a File object for the found resource, or null if resource not found.
	 * @throws IOException
	 */
	public static File fileViaClasspath(String fileBaseName) throws IOException {
		
		URL url = ClassLoader.getSystemClassLoader().getResource(fileBaseName);
		if (url == null)
			return null;
		String fullFilePath;
		if (url.getProtocol().equals("file")) {
			fullFilePath = url.getFile();        
		} else if (url.getProtocol().equals("jar")) {
			JarURLConnection jarUrl = (JarURLConnection) url.openConnection();
			fullFilePath = jarUrl.getJarFile().getName();            
		} else {
			throw new IllegalArgumentException("Not a file");
		}
		File file = new File(fullFilePath);
		return file;
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
		String hostname = "<unknown>";
		try {
			InetAddress addr = InetAddress.getLocalHost();
			// Get hostname
			hostname = addr.getHostName();
		} catch (UnknownHostException e) {
		}
		
		String errMsgBoilerPlate = infoOnError + " Host trying to connect: " + hostname + ". Connecting to: "; 
		
		while ((--attempts >= 0) && (resSock == null) || !resSock.isConnected()) {
			// Set up socket and input/output streams
			try {
				resSock = new Socket(machineName, port);
			} catch (UnknownHostException e) {
				String errMsg = errMsgBoilerPlate + machineName + ":" + port + "). Unknown host.";
				logger.error(errMsg);
				throw new IOException(errMsg);
			} catch (IOException e) {
				if (attempts > 0) {
					try {
						logger.warn("Socket open failed: " + errMsgBoilerPlate + machineName + ":" + port + ". Retrying...");
						Thread.sleep(1000 * Constants.SOCKET_OPEN_ATTEMPT_PAUSES[timeoutIndex++]);
					} catch (InterruptedException e1) {
						// ignore
					}
					// Try again:
					continue;
				}
				String errMsg = "Failed to open socket. " + errMsgBoilerPlate + machineName + ":" + port + ". " + e.getMessage();
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

	/*-----------------------------------------------------
	| getTaskID
	------------------------*/
	
	public static String getTaskID() {
			Configuration conf = UDFContext.getUDFContext().getJobConf();
			return conf.get("mapred.task.id");
	}
	
	/*-----------------------------------------------------
	| getTaskAttemptID
	------------------------*/
	
	public static TaskAttemptID getTaskAttemptID() {
		return TaskAttemptID.forName(getTaskID());
	}
	
	/*-----------------------------------------------------
	| getJobNum
	------------------------*/
	
	public static int getJobNumber() {
		TaskAttemptID id = TaskAttemptID.forName(getTaskID());
		return id.getJobID().getId();
	}

	/*-----------------------------------------------------
	| getTaskNum
	------------------------*/
	
	public static int getTaskNumber() {
		TaskAttemptID id = TaskAttemptID.forName(getTaskID());
		return id.getTaskID().getId();
	}
	
	/*-----------------------------------------------------
	| getTaskAttemptNum
	------------------------*/
	
	public static int getTaskAttemptNumber() {
		TaskAttemptID id = TaskAttemptID.forName(getTaskID());
		return id.getId();
	}
	
	/*-----------------------------------------------------
	| printUnixCommand
	------------------------*/
	
	public static void printUnixCommand(String cmd) {
        String s = null;

        try {
            
            Process p = Runtime.getRuntime().exec(cmd);
            
            BufferedReader stdInput = new BufferedReader(new 
                 InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new 
                 InputStreamReader(p.getErrorStream()));

            // read the output from the command
            System.out.println("Here is the standard output of the command:\n");
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
            
            // read any errors from the attempted command
            System.out.println("Here is the standard error of the command (if any):\n");
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }
            
            System.exit(0);
        }
        catch (IOException e) {
            System.out.println("Exception happened - here's what I know: ");
            e.printStackTrace();
            System.exit(-1);
        }
    }
	
	/*-----------------------------------------------------
	| runUnixCommand
	------------------------*/
	
	public static String runUnixCommand(String cmd) {
        String s = null;
        String stdout = "";
        @SuppressWarnings("unused")
		String stderr = ""; // Only used when debugging in Eclipse
        
        try {
            
            Process p = Runtime.getRuntime().exec(cmd);
            
            BufferedReader stdInput = new BufferedReader(new 
                 InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new 
                 InputStreamReader(p.getErrorStream()));

            // read the output from the command
            while ((s = stdInput.readLine()) != null) {
                stdout += s;
            }
            
            // read any errors from the attempted command
            while ((s = stdError.readLine()) != null) {
                stderr += s; // For debugging in Eclipse. Not printed or passed out.
            }
        }
        catch (IOException e) {
            System.out.println("Exception happened - here's what I know: " + e.getMessage());
            e.printStackTrace();
        }
        return stdout;
    }
	
	/*-----------------------------------------------------
	| getTupleIterator() 
	------------------------*/
	
	public static Iterator<Object> getTupleIterator(Tuple theTuple) {
		return (new Common()).new TupleIterator(theTuple);
	}
	
	
	/*-----------------------------------------------------
	| findVersionedFileName()
	------------------------*/
	
	/**
	 * Extract the 'first part' of a filename, given a directory, file name root, and extension.
	 * Example: Assuming an existing file "target/foo-1.0-SNAPSHOT.jar", and 
	 * given "target", "foo", and "jar", return "target/foo-1.0-SNAPSHOT.jar"
	 * @param dir: directory where file resides
	 * @param fileNameRoot: start of the filename, without the version.
	 * @param fileNameExt: file extension
	 * @return: extracted name, or error if unsuccessful.
	 * @throws IOException 
	 */
	public static String findVersionedFileName(String dir, String fileNameRoot, String fileNameExt) throws IOException {

	  File dirObj = new File(dir);
	    String[] fileNames = dirObj.list();
	  if(fileNames == null) {
		  throw new IOException("Specified directory does not exist or is not a directory.");
	  }
	  for (String fileName : fileNames) {
		  if (!fileName.startsWith(fileNameRoot) ||
		      !fileName.endsWith(fileNameExt))
		  	continue;
		  // Found the filename:
		  // File result = new File(dirObj, fileNameRoot + '.' + StringUtils.strip(fileNameExt, "."));
		  File result = new File(dirObj, fileName);
		  return result.getPath();
	  }
	  throw new IOException("No matching file name found in " + dir + " that starts with " + fileNameRoot + ", and ends with " + fileNameExt);
	}
	
	/*-----------------------------------------------------
	| removeLinks 
	------------------------*/
	
	/**
	 * Examples
	 *   	 "http://infolab" returns empty str
	 *  	 "https://infolab" returns empty str
	 *  	 "This is https://infolab a link." returns This is a link."
	 *  	 "http://infolab trailer." returns trailer.
     *
	 * @param inStr String with embedded HTTP links, but textual, not as tags.
	 * @return Text with http and https URLs removed.
	 */
	public static String removeLinks(String inStr) {
		String newStr = inStr.replaceAll("\\b(http://|https://)[^\\s]*\\b", "");
		return newStr;
	}

	/*-----------------------------------------------------
	| getTimestamp()
	------------------------*/
	
	public static long getTimestamp() {
		return new GregorianCalendar().getTimeInMillis();
	}
	
	
	// ---------------------------------   Support Classes ---------------------------
	
	public class TupleIterator implements Iterator<Object> {
		
		Tuple theTuple = null;
		int numObjsLeft = -1;
		int nextColumnIndex = 0;

		public TupleIterator(Tuple theTupleToIterate) {
			theTuple = theTupleToIterate;
			numObjsLeft = theTupleToIterate.size();
		}
		
		public boolean hasNext() {
			return numObjsLeft > 0;
		}

		public Object next() {
			try {
				numObjsLeft--;
				return theTuple.get(nextColumnIndex++);
			} catch (Exception e) {
				String errMsg = "Tuple iterator: ";
				if (theTuple.size() < 10)
					errMsg += "Tuple " + theTuple + " only has " + theTuple.size() + " elements."; 
				else
					errMsg += "Tuple only has " + theTuple.size() + " elements.";
						
				throw new NoSuchElementException(errMsg);
			}
		}

		public void remove() {
			throw new UnsupportedOperationException("The TupleIterator does not support the optional remove() method.");
			
		}
	}
}
