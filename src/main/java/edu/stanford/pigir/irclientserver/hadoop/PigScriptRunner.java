/**
 * 
 */
package edu.stanford.pigir.irclientserver.hadoop;

/**
 * @author paepcke
 *    
 *    TODO: thread should get hold of job ID, and make it available
 *    TODO: servicePigRequest needs to return job ID 
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.VisitorException;

import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigServiceHandle;
import edu.stanford.pigir.irclientserver.PigServiceImpl;

public class PigScriptRunner implements PigServiceImpl {

	private static String DEFAULT_SCRIPT_ROOT_DIR = "src/main/";
	private static String scriptRootDir = PigScriptRunner.DEFAULT_SCRIPT_ROOT_DIR;
	// Map from Pig script names (a.k.a. operators) to
	// their respective full pathnanmes:
	static Map<String,String> knownOperators = new HashMap<String,String>();
	
	Properties props = new Properties();
	Map<String,String> params = null;
	PigServer pserver;   // The Pig server that comes with Pig
	File scriptFile  = null;
	String outFilePath = null;
	String pigVar    = null;
	InputStream scriptInStream = null;
	PigRunThread runningThread = null;

	public PigScriptRunner() {
		// Used only if subsequently using the asynchronous servicePigRequest() method.
		// For constructors appropriate for synchronous use, see below.
	}
	
	public PigServiceHandle asyncPigRequest(String operator, Map<String, String> theParams) {
		
		// Build a jobname
		String jobName = "arcspread_" + PigServiceHandle.getPigTimestamp();
		PigServiceHandle resultHandle = new PigServiceHandle(jobName, JobStatus.RUNNING);
		
		String pigScriptPath = knownOperators.get(operator);
		if (pigScriptPath == null) {
			// If this script name is not found, try updating
			// our cached list of scripts in known places:
			initAvailablePigScripts();
			pigScriptPath = knownOperators.get(operator);
		}
		// Script still not found?
		if (pigScriptPath == null)  {
			String errMsg = String.format("Pig request '%s' is not known; no Pig script implementation found.", operator);
			Log.error(errMsg);
			return new PigServiceHandle(jobName, JobStatus.FAILED, errMsg);
		}
		params = theParams;
		PigScriptRunner runner = null;
		try {
			runner = new PigScriptRunner(new File(pigScriptPath), params);
		} catch (IOException e) {
			String errMsg = String.format("Pig request '%s': could not start PigScriptRunner with script '%s'; reason: %s", 
								           operator, pigScriptPath, e.getMessage());
			Log.error(errMsg);
			return new PigServiceHandle(jobName, JobStatus.FAILED, errMsg);
		}
		
		// Start a new thread to run this script:
		createPigServer();
		PigRunThread scriptThread = runner.new PigRunThread(); 
		scriptThread.start(runner, pserver, params);
		runner.runningThread = scriptThread;
		
		//PigContext context = pserver.getPigContext();
		//Properties props = context.getProperties();
		
		return resultHandle;
	}
	
	
	public PigScriptRunner(File theScriptFile, Map<String,String> theParams) throws IOException {
		ensureScriptFileOK(theScriptFile);
		params = theParams;
	}
	
	public PigScriptRunner(File theScriptFile, String theVarToIterate, Map<String,String> theParams) throws IOException {
		ensureScriptFileOK(theScriptFile);
		if (theVarToIterate == null) {
		   throw new IllegalArgumentException("Script variable to print or iterate over must be provided; null was passed instead.");
		}
		pigVar = theVarToIterate;
		params = theParams;
	}
	
	public PigScriptRunner(File theScriptFile, String theOutfile, String varToStore, Map<String,String> theParams) throws IOException {
		ensureScriptFileOK(theScriptFile);
		if (varToStore == null) {
			throw new IllegalArgumentException("Script variable to store must be provided; null was passed instead.");
		}
		
		if (theOutfile == null) {
			throw new IllegalArgumentException("Output file name must be provided; null was passed instead.");
		}
		outFilePath = theOutfile;
		pigVar  = varToStore;
		params = theParams;
	}	
	
	public void addScriptCallParam(String paramName, String paramValue) {
		props.setProperty(paramName, paramValue);
	}
	
	public Iterator<Tuple> iterator() throws IOException {
		createPigServer();
		pserver.registerScript(scriptInStream, params);
		return pserver.openIterator(pigVar);
	}

	public boolean store() throws IOException {
		if (outFilePath == null) {
			Log.error("Output file path is null; use one of the PigScriptRunner constructors that take an out file.");
			return false;
		}
		createPigServer();
		pserver.registerScript(scriptInStream, params);
		try {
			pserver.store(pigVar, outFilePath);
		} catch (PigException e) {
			if ((e.getCause() instanceof VisitorException) && e.getCause().getCause() instanceof FileAlreadyExistsException) {
				// This 'error' occurs when a script contains a store() statement.
				// Without the pserver.store() above the script's store is ignored.
				// But when we do call pserver.store(), the script's store is executed,
				// creating the output file. The pserver.store() then wants to write
				// again but fails b/c the output file already exists. So, we
				// just ignore that message.
				return true;
			} else {
			Log.error("During pigserver 'store': " + e.getMessage());
			return false;
			}
		}
		return true;
	}

	public void shutDownPigRequest() {
		if (pserver != null)
			pserver.shutdown();
	}
	
	private void ensureScriptFileOK(File theScriptFile) throws FileNotFoundException {
		if (theScriptFile == null) {
			throw new IllegalArgumentException("Script file must be provided; null was passed instead.");
		}
		if (! theScriptFile.canRead()) {
			throw new IllegalArgumentException("Script file not found or not readable: " + theScriptFile.getAbsolutePath());
		}
		scriptFile = theScriptFile;
		scriptInStream = new FileInputStream(theScriptFile);		
	}
	
	private void createPigServer() {
		if (pserver != null)
			return;
		if (props.getProperty("pig.exectype") == null)
			props.setProperty("pig.exectype", "local");
		props.setProperty("pig.usenewlogicalplan", "false");
		String userHome = System.getenv("HOME");
		if (userHome == null)
			props.setProperty("pig.temp.dir","/tmp");
		else
			props.setProperty("pig.temp.dir", userHome);
		props.setProperty("PIG_HOME",System.getenv("PIG_HOME"));
		String pigUserJarDir = System.getenv("PIG_USER_JAR_DIR");
		if (pigUserJarDir == null)
			pigUserJarDir = "target";
		props.setProperty("USER_CONTRIB", pigUserJarDir);
		props.setProperty("PIGIR_HOME", System.getenv("PIGIR_HOME"));
		try {
			if (props.get("pig.exectype") == "local")
				pserver = new PigServer(ExecType.LOCAL, props);
			else
				pserver = new PigServer(ExecType.MAPREDUCE, props);
			pserver.debugOn();
		} catch (ExecException e) {
			Log.error("Thread could not start a PigServer instance: " + e.getMessage());
			return;
		}
	}
	
	/**
	 * Check in well-known place for any new Pig scripts.
	 * Add their file basenames to a lookup table, so that
	 * remote callers can only use those basenames. I.e. /foo/bar/myScript.pig ==> myScript
	 */
	private static void initAvailablePigScripts() {
		
		File[] files = new File(FilenameUtils.concat(PigScriptRunner.scriptRootDir, "PigScripts/CommandLineUtils/Pig/")).listFiles();
		if (files == null) {
			Log.error("Found no Pig scripts in " + FilenameUtils.concat(PigScriptRunner.scriptRootDir, "PigScripts/CommandLineUtils/Pig/"));
			return;
		}
		for (File file : files) {
			if (file.isFile()) {
				knownOperators.put(FilenameUtils.getBaseName(file.getAbsolutePath()), file.getAbsolutePath());
			}
		}	
	}

	@Override
	public PigServiceHandle getProgress(PigServiceHandle service) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * Used by unit tests to point to the root of the Maven 'test' 
	 * branch rather than the 'main' branch.
	 * @param packageRoot
	 */
	public void setScriptRootDir(String scriptRoot) {
		PigScriptRunner.scriptRootDir = scriptRoot;
	}
	
	class PigRunThread extends Thread {

		PigScriptRunner parent = null;
		boolean keepRunning    = true;
		PigServer pserver      = null;
		Map<String,String> params = null;
		
		public void start(PigScriptRunner threadStarter, PigServer thePserver, Map<String,String> theParams) {
			super.start();
			parent  = threadStarter;
			pserver = thePserver;
			params  = theParams;
		}

		public void run() {
			
			try {
				try {
					scriptInStream = new FileInputStream(scriptFile);
				} catch (FileNotFoundException e) {
					// We checked for this before instantiating the thread:
					e.printStackTrace();
				}
				try {
					pserver.registerScript(scriptInStream, params);
				} catch (IOException e) {
					String errMsg = String.format("Cannot run script '%s': %s", scriptFile, e.getMessage());
					Log.error(errMsg);
					return;
				}
			} finally {
				shutdown();
			}
		}
				
		
/*		public void run() {
			// This version of run feeds one script line after another
			// to the Pig server.
			PigScriptReader scriptReader;
			try {
				try {
					if (scriptFile != null) {
						scriptReader = new PigScriptReader(scriptFile);
					} else {
						scriptReader = new PigScriptReader(scriptInStream);
					}
				} catch (FileNotFoundException e) {
					String errMsg;
					try {
						errMsg = String.format("Pig script file '%s' not found on host %s.",
								scriptFile, InetAddress.getLocalHost().getHostName());
					} catch (UnknownHostException e1) {
						errMsg = String.format("Pig script file '%s' not found on Pig host.", scriptFile);
					}
					Log.error(errMsg);
					return;
				}
				// TODO: try running a script file again, then pass in parameters:
				while (scriptReader.hasNext() && keepRunning) {
					String scriptCodeLine = scriptReader.next();
					try {
						pserver.registerQuery(scriptCodeLine);
					} catch (IOException e) {
						String errMsg = String.format("Cannot submit Pig code line '%s' to the Pig server; aborting script run: %s",
								scriptCodeLine, e.getMessage());
						Log.error(errMsg);
						return;
					}
				}
			} finally {
				shutdown();
			}
		}
*/		
		public void shutdown() {
			keepRunning = false;
			shutDownPigRequest();
			Log.info("Pig script server thread shutting down cleanly upon request.");
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}


}
