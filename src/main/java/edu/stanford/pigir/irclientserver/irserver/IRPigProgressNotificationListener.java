/**
 * 
 */
package edu.stanford.pigir.irclientserver.irserver;

import org.apache.pig.tools.pigstats.PigStats;

/**
 * @author paepcke
 *
 */
public interface IRPigProgressNotificationListener extends org.apache.pig.tools.pigstats.PigProgressNotificationListener {

	/**
	 * Starting a Pig script via PigRunner provides a return
	 * code (see org.apache.pig.PigRunner.ReturnCode). For example,
	 * the use of an unknown exectype causes a non-SUCCESS return code.
	 * @return Pig script startup return value.
	 */
	public int getPigStartReturnCode();
	
	
	/**
	 * Starting a Pig script via PigRunner provides an
	 * error message, if one occurs
	 * @return Pig script startup error message, if any.
	 */
	public String getErrorMessage();

	
	
	/**
	 * Use to provide the notification listener with
	 * the PigStats returned by PigRunner when a script
	 * is started. The data structure is used for return code
	 * and error message retrieval.
	 * @param thePigStats
	 */
	public void setJobPigStats(PigStats thePigStats);

	/**
	 * Return the latest progress percentage number.
	 * @return percentage complete.
	 */
	public int getProgress();
	
	/**
	 * Get total number of jobs that are still unfinished.
	 * @return number of unfinished Map/Reduce jobs.
	 */
	public int getNumSubjobsRunning();
	
	/**
	 * Return either total amount of time taken to finish
	 * the Pig job or, if job is not yet finished, return
	 * runtime so far:
	 * @return
	 */
	public long getRuntime();
	
	public long getLatestActivityTime();
	
	public long getBytesWritten();
	
	public boolean getJobComplete();
	
}
