/**
 * 
 */
package edu.stanford.pigir.irclientserver.hadoop;

/**
 * @author paepcke
 *    
 *    TODO: Ensure that very old PigProgressListener instances are eventually removed from PigScriptRunner.progressListeners.
 *    TODO: Provide cross-network access to the IRServer's Iterator method.
 *    TODO: ngrams are put deep under ~/EclipseWorkspaces/PigIR/home/... 
 *    TODO: continue in TestIRLib: hang on result queue.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigRunner;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;

import edu.stanford.pigir.irclientserver.ArcspreadException;
import edu.stanford.pigir.irclientserver.IRServConf;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigServiceHandle;
import edu.stanford.pigir.irclientserver.PigServiceImpl_I;
import edu.stanford.pigir.irclientserver.PigService_I;

/**
 * @author paepcke
 * 
 * Class to invoke particular Pig scripts. The class is
 * instantiated for each Pig script run. These instances
 * normally operate on the server side of a client-server
 * arrangement. The instances are created from IRServer.java.   
 * The class provides several options for running scripts. 
 * <ul>
 * <li>Invoke asynchronously via asyncPigRequest(), and get
 *     progress information via getProgress(). Use the argument-less
 *     constructor for this mode.
 *     </li>
 * <li>Create an iterator that will provide successive tuples
 *     of the Pig run. Use one of the constructors that take
 *     arguments, and then call the iterator() method.
 *     </li>
 * <li>Invoke a Pig script that does not contain a DUMP or STORE
 *     statement, thereby 'staging' it. Pig will not run such a script.
 *     Then call the store() method on your PigScriptRunner instance to
 *     kick off the run. (likely less useful option; might be removed in future). 
 * </ul>
 *
 */
public class PigScriptRunner implements PigServiceImpl_I {

	public static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.hadoop");	

	private static String scriptRootDir = IRServConf.DEFAULT_SCRIPT_ROOT_DIR;
	// Map from Pig script names (a.k.a. operators) to
	// their respective full pathnanmes:
	static Map<String,String> knownOperators = new HashMap<String,String>();
	
	// Keeping track of PigProgressListeners that we started in
	// asyncPigRequest():
	private static Map<String,PigProgressListener> progressListeners = new HashMap<String,PigProgressListener>();
	
	Properties props = new Properties();
	Map<String,String> params = new HashMap<String,String>();
	PigServer pserver;   // The Pig server that comes with Pig
	PigService_I responseCallbackDest = null;
	File scriptFile  = null;
	String outFilePath = null;
	String pigVar    = null;
	InputStream scriptInStream = null;
	PigRunThread runningThread = null;

	public PigScriptRunner() {
		// This constructor is used directly by clients when the asynchronous servicePigRequest() method.
		// is subsequently used. For constructors appropriate for synchronous use, see below.
		BasicConfigurator.configure();
	}
				
	public PigScriptRunner(File theScriptFile, Map<String,String> theParams) throws IOException {
		this();
		ensureScriptFileOK(theScriptFile);
		if (params != null)
			params = theParams;
	}
	
	public PigScriptRunner(File theScriptFile, String theVarToIterate, Map<String,String> theParams) throws IOException {
		this();
		ensureScriptFileOK(theScriptFile);
		if (theVarToIterate == null) {
		   throw new IllegalArgumentException("Script variable to print or iterate over must be provided; null was passed instead.");
		}
		pigVar = theVarToIterate;
		if (params != null)
			params = theParams;
	}
	
	public PigScriptRunner(File theScriptFile, String theOutfile, String varToStore, Map<String,String> theParams) throws IOException {
		this();
		ensureScriptFileOK(theScriptFile);
		if (varToStore == null) {
			throw new IllegalArgumentException("Script variable to store must be provided; null was passed instead.");
		}
		
		if (theOutfile == null) {
			throw new IllegalArgumentException("Output file name must be provided; null was passed instead.");
		}
		outFilePath = theOutfile;
		pigVar  = varToStore;
		if (params != null)
			params = theParams;
	}	
	
	public JobHandle_I asyncPigRequest(String operator, Map<String, String> theParams) {
		return asyncPigRequest(operator, theParams, null); // no object to call back to with responses
	}
	public JobHandle_I asyncPigRequest(String operator, Map<String, String> theParams, PigService_I theResponseCallbackDest) {
		
		// Build a human-readable jobname to pass back to
		// caller for referencing this job:
		String jobName = "arcspread_" + PigServiceHandle.getPigTimestamp();
		PigServiceHandle jobHandle = new PigServiceHandle(jobName, JobStatus.RUNNING);
		responseCallbackDest = theResponseCallbackDest;
		
		String pigScriptPath = knownOperators.get(operator);
		if (pigScriptPath == null) {
			// If this script name is not found, try updating
			// our cached list of scripts in known places:
			initAvailablePigScripts();
			pigScriptPath = knownOperators.get(operator);
		}
		
		// Script still not found?
		if (pigScriptPath == null)
			return new ArcspreadException.NotImplementedException(String.format("Pig request '%s' is not known; no Pig script implementation found.", operator));
		if (params != null)
			params = theParams;
		
		// If the path to the Pigscript is empty, this means
		// the implementation of the operation is within this
		// Runner class. That is true in particular for the
		// testCall() method:
		if (pigScriptPath.length() == 0) {
			switch (operator) {
			case "testCall":
				if ((params == null) || params.get("msgToEcho") == null)
					return new ArcspreadException.ParameterException("Test procedure testCall() requires parameter 'msgToEcho', which was not provided.");
				return testCall(params.get("msgToEcho"));
			default:
				return new ArcspreadException.NotImplementedException(String.format("Pig request '%s' is not known; no Pig script implementation found.", operator));
			}
		}
		
		PigScriptRunner runner = null;
		try {
			runner = new PigScriptRunner(new File(pigScriptPath), params);
		} catch (IOException e) {
			String errMsg = String.format("Pig request '%s': could not start PigScriptRunner with script '%s'; reason: %s", 
								           operator, pigScriptPath, e.getMessage());
			log.error(errMsg);
			return new PigServiceHandle(jobName, JobStatus.FAILED, errMsg);
		}
		
		// Start a new thread to run this script:
		PigRunThread scriptThread = runner.new PigRunThread();
		PigProgressListener progressListener = new PigProgressListener(this, jobHandle);
		PigScriptRunner.progressListeners.put(jobName, progressListener);
		scriptThread.start(progressListener, pigScriptPath, params);
		runner.runningThread = scriptThread;
		
		return jobHandle;
	}

	/* (non-Javadoc)
	 * @see edu.stanford.pigir.irclientserver.PigServiceImpl_I#getProgress(java.lang.String)
	 */
	public JobHandle_I getProgress(String jobName) {
		return getProgress(new PigServiceHandle(jobName, JobStatus.UNKNOWN));
	}
	
	/* (non-Javadoc)
	 * @see edu.stanford.pigir.irclientserver.PigServiceImpl_I#getProgress(edu.stanford.pigir.irclientserver.JobHandle_I)
	 */
	@Override
	public JobHandle_I getProgress(JobHandle_I jobHandle) {
		String jobName = jobHandle.getJobName();
		PigProgressListener listener = PigScriptRunner.progressListeners.get(jobName);
		if (listener == null) {
			jobHandle.setStatus(JobStatus.UNKNOWN);
			return jobHandle;
		}

		// Did the script run call to PigRunner in the thread below
		// finish? If so, what was the outcome?
		int pigReturnErrorCode = listener.getPigStartReturnCode();
		
		// If no return code is available, then the thread is
		// still working on submitting the Pig script:
		if (pigReturnErrorCode == PigRunner.ReturnCode.UNKNOWN) {
			jobHandle.setStatus(JobStatus.PREP);
			return jobHandle;
		}
		
		// Did Pig script startup fail?
		if (pigReturnErrorCode != PigRunner.ReturnCode.SUCCESS) {
			ArcspreadException.PreHadoopException retExc =
					new ArcspreadException.PreHadoopException(jobHandle.getJobName(),
															  listener.getErrorMessage());
			return retExc;
		}
		
		// Try to detect where the Pig processor found an error
		// before submitting any work to Hadoop. Example: No Hadoop
		// config file found, nor "-x local" specified. In that case
		// Pig never calls any of the callbacks in PigProgressListener,
		// so we use heuristics:
		if ((listener.getLatestActivityTime() == -1) &&
			(listener.getRuntime() > IRServConf.STARTUP_TIME_MAX)) {
			jobHandle = new ArcspreadException.PreHadoopException(jobHandle.getJobName(), "Could not start Hadoop; check server side log for reason (maybe hadoop-site.xml nor core-site.xml in classpath? If so, put 'exectype'->'local' into parameter map)");
			jobHandle.setStatus(JobStatus.FAILED);
			return jobHandle;
		}
		
		jobHandle.setProgress(listener.getProgress());
		jobHandle.setNumJobsRunning(listener.getNumSubjobsRunning());
		jobHandle.setRuntime(listener.getRuntime()); // runtime so far
		jobHandle.setBytesWritten(listener.getBytesWritten());
		if (listener.getJobComplete())
			jobHandle.setStatus(JobStatus.SUCCEEDED);
		else
			jobHandle.setStatus(JobStatus.RUNNING);
		
		return jobHandle;
	}
	
    //-------------------------   End Asyncronous Script Invocation Facilities -------------------	
	
	//-------------------------   Begin syncronous Script Invocation Facilities -------------------	
	
	/**
	 * Add a parameter to the params passed in when creating this instance
	 * @param paramName
	 * @param paramValue
	 */
	public void addScriptCallParam(String paramName, String paramValue) {
		props.setProperty(paramName, paramValue);
	}
	
	/**
	 * Obtain an iterator over the script result tuples
	 * @return tuple iterator; use standard hasNext()/next() calls on it.
	 * @throws IOException
	 */
	public Iterator<Tuple> iterator() throws IOException {
		createPigServer();
		pserver.registerScript(scriptInStream, params);
		return pserver.openIterator(pigVar);
	}

	/**
	 * If script does not include a dump or store command, Pig won't
	 * execute at all. In that case, must use this method to trigger
	 * execution.
	 * @return success indicator.
	 * @throws IOException
	 */
	public boolean store() throws IOException {
		if (outFilePath == null) {
			log.error("Output file path is null; use one of the PigScriptRunner constructors that take an out file.");
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
			log.error("During pigserver 'store': " + e.getMessage());
			return false;
			}
		}
		return true;
	}

	/**
	 * Free Pig Server resources.
	 */
	public void shutDownPigRequest() {
		if (pserver != null)
			pserver.shutdown();
	}
	
	//-------------------------   T E S T I N G -------------------	
	
	public PigServiceHandle testCall(String strToEcho) {
		PigServiceHandle resultHandle = new PigServiceHandle("FakeTestJobName", JobStatus.SUCCEEDED);
		resultHandle.setMessage(new Date().toString() + ":" + strToEcho);
		return resultHandle;
	}

	//-------------------------   P R I V A T E  or I N T E R N A L  U S E  -------------------
	
	/* (non-Javadoc)
	 * @see edu.stanford.pigir.irclientserver.PigServiceImpl_I#reportLaunchStatus(edu.stanford.pigir.irclientserver.JobHandle_I)
	 */
	public void reportLaunchStatus(JobHandle_I jobHandle) {
		if (responseCallbackDest == null)
			return;
		responseCallbackDest.pushResultNotification(jobHandle);
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
			log.error("Thread could not start a PigServer instance: " + e.getMessage());
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
			log.error("Found no Pig scripts in " + FilenameUtils.concat(PigScriptRunner.scriptRootDir, "PigScripts/CommandLineUtils/Pig/"));
			return;
		}
		for (File file : files) {
			if (file.isFile()) {
				knownOperators.put(FilenameUtils.getBaseName(file.getAbsolutePath()), file.getAbsolutePath());
			}
		}	
		// The special callTest() "script". It's actually not a Pig script, but
		// a loopback test handled within PigScriptRunner: associate this method
		// with an empty script path:
		knownOperators.put("testCall", "");
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

		PigProgressNotificationListener progressListener = null;
		boolean keepRunning    = true;
		Map<String,String> params = null;
		List<String> cmd = new ArrayList<String>();
		PigStats pigStats = null;
		String pigScriptPath = null;
		
		public void start(PigProgressNotificationListener theProgressListener, String thePigScriptPath,  Map<String,String> theParams) {
			super.start();
			progressListener  = theProgressListener;
			params  = theParams;
			pigScriptPath = thePigScriptPath;
			if (params == null)
				params = new HashMap<String,String>();
			// Create the commandline call:
			for (String paramKey : params.keySet()) {
				if (paramKey.equals("exectype")) {
					cmd.add("-x");
					cmd.add(params.get(paramKey));
					continue;
				} else {
					cmd.add("-param");
					cmd.add(paramKey + "=" + params.get(paramKey));
				}
			}
			cmd.add("-f");
			cmd.add(pigScriptPath);
		}

		public void run() {
			// Note: PigRunner does not throw exceptions, just logs them,
			// and notes them in its returned PigStats. The Exception catch
			// is never invoked. But the try is still useful for its finally
			// clause.
			try {
				String[] cmdArr = new String[cmd.size()];
				cmd.toArray(cmdArr);
				pigStats = PigRunner.run(cmdArr, progressListener);
				// Make this job's stats available to the progress listener:
				((PigProgressListener)progressListener).setJobPigStats(pigStats);
			} catch (Exception e) {
				PigScriptRunner.log.error(String.format("Error while running script %s: %s", pigScriptPath, e.getMessage()));
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
					log.error(errMsg);
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
						log.error(errMsg);
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
			log.info("Pig script server thread shutting down cleanly upon request.");
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}


}
