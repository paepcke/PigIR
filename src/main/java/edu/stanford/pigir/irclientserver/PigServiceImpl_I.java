package edu.stanford.pigir.irclientserver;

import java.util.Map;

public interface PigServiceImpl_I {

	// Must return quickly! Called from network service. Returns
	// identifier to use for subsequent status requests.
	/**
	 * Submit a script run request that includes the script name,
	 * and a Map of parameter/value pairs to pass into the script.
	 * Note: Must return quickly! Called from network service. 
	 * Returns identifier to use for subsequent status requests.
	 * @param operator name of the (Pig) script 
	 * @param params all parameters name-->value required by the script
	 * @return a new JobHandle_I instance for use in subsequent calls to getProgress()
	 */
	JobHandle_I asyncPigRequest(String operator, Map<String, String> params, PigService_I notificationCallbackObj);
	
		// -------------------------  Public API ------------------------
	
	/**
	 * Return selected status information about a Pig launch
	 * @param jobName the job name that was returned in the JobHandle_I by asyncPigReques().
	 * @return the passed-in JobHandle_i object with updated information
	 */
	public JobHandle_I getProgress(String jobName);
	
	/**
	 * Return selected status information about a Pig launch
	 * @param service the JobHandle_I object that was returned by asyncPigReques()
	 * @return the passed-in JobHandle_i object with updated information
	 */
	public JobHandle_I getProgress(JobHandle_I service);
	
	/**
	 * Specify the root directory from which Pig script locations are
	 * reckoned. Only used if Pig scripts are in a place other than the
	 * usual <projectRoot>/src/main/PigScripts/CommandLine/Pig
	 * @param scriptRoot
	 */
	public void setScriptRootDir(String scriptRoot);
	
	
	// -------------------------  Internal Use ------------------------
	
	/**
	 * Called by PigProgressListener when an entire launch has finished.
	 * Used to provide push-callback service for client side applications.
	 * @param jobHandle
	 */
	public void reportLaunchStatus(JobHandle_I jobHandle);
	
	/**
	 * For testing class PigScriptRunner without involving a client-server
	 * exchange. Returns a given string in the message field of the
	 * JobHandle_I.
	 * @param strToEcho test string to return
	 * @return JobHandle_I with the message field containing strToEcho
	 */
	public JobHandle_I testCall(String strToEcho);
}
