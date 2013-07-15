/**
 * 
 */
package edu.stanford.pigir.irclientserver.hadoop;

/**
 * @author paepcke
 *
 *    TODO:
 *        - thread should get hold of job ID, and make it available
 *        - servicePigRequest needs to return job ID 
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.PigService;
import edu.stanford.pigir.irclientserver.Utils;
import edu.stanford.pigir.irclientserver.irserver.IRServer;


public class PigScriptRunner implements PigService {

	private static PigServer pserver;   // The Pig server that comes with Pig
	private static IRServer irServer = null;
	
	private static String packageRootDir = "src/main/java";
	// Map from Pig script names (a.k.a. operators) to
	// their respective full pathnanmes:
	static Map<String,String> knownOperators = new HashMap<String,String>();
	
	
	Properties props = new Properties();
	File scriptFile  = null;
	String outFilePath = null;
	String pigVar    = null;
	InputStream scriptInStream = null;

	public static PigServiceID servicePigRequest(String operator, Map<String, String> params) {
		String pigScriptPath = knownOperators.get(operator);
		if (pigScriptPath == null) {
			String errMsg = String.format("Pig request '%s' is not known; no Pig script implementation found.", operator);
			Log.error(errMsg);
			return null;
		}
		String pigResultAlias = null;
		try {
			pigResultAlias = Utils.getPigResultAlias(pigScriptPath);
		} catch (IOException e) {
			String errMsg = String.format("Pig request '%s': could not open script '%s'", 
								           operator, pigScriptPath);
			Log.error(errMsg);
			return null;
		}
		if (pigResultAlias == null) {
			String errMsg = String.format("Pig request '%s': could not find Pig result alias declaration at top of script file '%s'", 
								           operator, pigScriptPath);
			Log.error(errMsg);
			return null;
		}
		PigScriptRunner runner = null;
		try {
			runner = new PigScriptRunner(new File(pigScriptPath), pigResultAlias);
		} catch (IOException e) {
			String errMsg = String.format("Pig request '%s': could not start PigScriptRunner with script '%s'; reason: %s", 
								           operator, pigScriptPath, e.getMessage());
			Log.error(errMsg);
			return null;
		}
		new PigRunThread().start();
		
	}
	
	
	public PigScriptRunner(File theScriptFile, String theVarToPrintOrIterate) throws IOException {
		if (theScriptFile == null) {
			throw new IllegalArgumentException("Script file must be provided; null was passed instead.");
		}
		
		if (theVarToPrintOrIterate == null) {
			throw new IllegalArgumentException("Script variable to print or iterate over must be provided; null was passed instead.");
		}
		
		scriptFile = theScriptFile;
		scriptInStream = new FileInputStream(theScriptFile);		
		pigVar = theVarToPrintOrIterate;
		initPig();
	}
	
	public PigScriptRunner(File theScriptFile, String theOutfile, String varToStore) throws IOException {
		if (theScriptFile == null) {
			throw new IllegalArgumentException("Script file must be provided; null was passed instead.");
		}
		
		if (varToStore == null) {
			throw new IllegalArgumentException("Script variable to store must be provided; null was passed instead.");
		}
		
		if (theOutfile == null) {
			throw new IllegalArgumentException("Output file name must be provided; null was passed instead.");
		}
		
		scriptFile = theScriptFile;
		scriptInStream = new FileInputStream(theScriptFile);
		outFilePath = theOutfile;
		pigVar  = varToStore;
		initPig();
	}

	public PigScriptRunner(InputStream theScriptStream, String theOutfile, String varToStore) throws IOException {
		if (theScriptStream== null) {
			throw new IllegalArgumentException("Script stream must be provided; null was passed instead.");
		}
		
		if (varToStore == null) {
			throw new IllegalArgumentException("Script variable to store must be provided; null was passed instead.");
		}
		
		if (theOutfile == null) {
			throw new IllegalArgumentException("Output file name must be provided; null was passed instead.");
		}
		
		scriptInStream = theScriptStream;
		outFilePath = theOutfile;
		pigVar  = varToStore;
		initPig();
	}
	
	public PigScriptRunner(InputStream theScriptStream, String theVarToPrintOrIterate) throws IOException {
		scriptInStream = theScriptStream;
		pigVar  = theVarToPrintOrIterate;
		initPig();
	}
	
	
	@Override
	public String getProgress(PigServiceID service) {
		// TODO Auto-generated method stub
		return null;
	}

	public void addScriptCallParam(String paramName, String paramValue) {
		props.setProperty(paramName, paramValue);
	}
	
	public Iterator<Tuple> iterator() throws IOException {
		startServer();
		pserver.registerScript(scriptInStream);
		return pserver.openIterator(pigVar);
	}
	
	public void store() throws IOException {
		startServer();
		pserver.registerScript(scriptInStream);
		pserver.store(pigVar, outFilePath);
	}
	
	public void shutdown() {
		if (pserver != null) {
			pserver.shutdown();
		}
	}
	
	private void initPig() throws IOException {
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
		irServer = IRServer.getInstance();
	}
	
	private static void initAvailablePigScripts() {
		
		File[] files = new File(FilenameUtils.concat(PigScriptRunner.packageRootDir, "PigScripts/CommandLineUtils/Pig/")).listFiles();
		for (File file : files) {
			if (file.isFile()) {
				knownOperators.put(FilenameUtils.getBaseName(file.getAbsolutePath()), file.getAbsolutePath());
			}
		}	
	}
	
	private void startServer() {
		try {
			//pserver = new PigServer(ExecType.MAPREDUCE, props);
			pserver = new PigServer(ExecType.LOCAL, props);
			pserver.debugOn();
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Used by unit tests to point to the root of the Maven 'test' 
	 * branch rather than the 'main' branch.
	 * @param packageRoot
	 */
	public static void setPackageRootDir(String packageRoot) {
		PigScriptRunner.packageRootDir = packageRoot;
	}
	
	class PigRunThread extends Thread {
		
		public void run() {
			startServer();
			PigScriptReader scriptReader;
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
			while (scriptReader.hasNext()) {
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
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
