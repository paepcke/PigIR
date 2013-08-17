/**
 * 
 */
package edu.stanford.pigir.irclientserver.hadoop;

import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * @author paepcke
 *
 * Every Pig script is started with an instance of this
 * listener. This instance allows for monitoring of progress
 * over the wire. PigScriptRunner creates the instances, and
 * keeps track of them.
 * 
 */
public class PigProgressListener implements org.apache.pig.tools.pigstats.PigProgressNotificationListener {

	// One logger for all progress listeners:
	public static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.hadoop");
	
	long startTime = System.currentTimeMillis();
	long endTime = -1;
	String scriptID = "";
	
	PigStats jobPigStats = null;
	boolean jobComplete = false;
	int progress = 0;
	int numJobsCompleted = 0;
	
	int numJobsToLaunch = 0;
	int numJobsSubmitted = 0;
	String assignedJobId = "";
	JobStats finishedJobStats = null;
	JobStats failedJobStats = null;
	OutputStats outputStats = null;

	public PigProgressListener() {
		BasicConfigurator.configure();
	}

	/**
	 * Called from thread in PigScriptRunner. That thread receives
	 * the PigStats for the Pig job that it submits to the Pig
	 * server. 
	 * @param thePigStats
	 */
	public void setJobPigStats(PigStats thePigStats) {
		jobPigStats = thePigStats;
	}
	
	/**
	 * Return the latest progress percentage number.
	 * @return percentage complete.
	 */
	public int getProgress() {
		return progress;
	}
	
	/**
	 * Get total number of jobs that are still unfinished.
	 * @return number of unfinished Map/Reduce jobs.
	 */
	public int getNumSubjobsRunning() {
		return numJobsToLaunch - numJobsCompleted;
	}
	
	/**
	 * Return either total amount of time taken to finish
	 * the Pig job or, if job is not yet finished, return
	 * runtime so far:
	 * @return
	 */
	public long getRuntime() {
		long timeNow = System.currentTimeMillis();
		if (endTime > -1)
			return (endTime - startTime);
		else
			return (timeNow - startTime);
	}
	
	public long getBytesWritten() {
		if ((jobPigStats == null) && (outputStats == null))
			return -1;
		if (outputStats != null)
			return outputStats.getBytes();
		List<OutputStats> allOutStats = jobPigStats.getOutputStats();
		long written = 0;
		for (OutputStats outStats : allOutStats) {
			written += outStats.getBytes();
		}
		return written;
	}
	
	/**
     * Invoked just before launching MR jobs spawned by the script.
     * @param scriptId the unique id of the script
     * @param numJobsToLaunch the total number of MR jobs spawned by the script
     */
    public void launchStartedNotification(String scriptId, int theNumJobsToLaunch) {
    	numJobsToLaunch += theNumJobsToLaunch;
    	PigProgressListener.log.info(String.format("Launched; scriptID: %s; numJobsLaunched: %d", scriptId, theNumJobsToLaunch));    	
    }
    
    /**
     * Invoked just before submitting a batch of MR jobs.
     * @param scriptId the unique id of the script
     * @param numJobsSubmitted the number of MR jobs in the batch
     */
    public void jobsSubmittedNotification(String scriptId, int theNumJobsSubmitted) {
    	numJobsSubmitted += theNumJobsSubmitted;
    	PigProgressListener.log.info(String.format("Submitted; scriptID: %s; numJobsSubmitted: %d", scriptId, theNumJobsSubmitted));    	
    }
    
    /**
     * Invoked after a MR job is started.
     * @param scriptId the unique id of the script 
     * @param assignedJobId the MR job id
     */
    public void jobStartedNotification(String scriptId, String theAssignedJobId) {
    	assignedJobId = theAssignedJobId;
    	PigProgressListener.log.info(String.format("Started; scriptID: %s; jobID: %s", scriptId, theAssignedJobId));    	
    }
    
    /**
     * Invoked just after a MR job is completed successfully. 
     * @param scriptId the unique id of the script 
     * @param jobStats the {@link JobStats} object associated with the MR job
     */
    public void jobFinishedNotification(String scriptId, JobStats theFinishedJobStats) {
    	finishedJobStats = theFinishedJobStats;
    	//PigProgressListener.log.String.format("Finished; scriptID: %s; jobStats: %s", scriptId, theFinishedJobStats);
    }
    
    /**
     * Invoked when a MR job fails.
     * @param scriptId the unique id of the script 
     * @param jobStats the {@link JobStats} object associated with the MR job
     */
    public void jobFailedNotification(String scriptId, JobStats theFailedJobStats) {
    	failedJobStats = theFailedJobStats;
    	PigProgressListener.log.info(String.format("Failed; scriptID: %s; jobStats: %s", scriptId, theFailedJobStats));    	
    }
    
    /**
     * Invoked just after an output is successfully written.
     * @param scriptId the unique id of the script
     * @param outputStats the {@link OutputStats} object associated with the output
     */
    public void outputCompletedNotification(String scriptId, OutputStats theOutputStats) {
    	outputStats = theOutputStats;
    	PigProgressListener.log.info(String.format("Output complete; scriptID: %s; outputStats: %s", scriptId, theOutputStats));
    }
    
    /**
     * Invoked to update the execution progress. 
     * @param scriptId the unique id of the script
     * @param progress the percentage of the execution progress
     */
    public void progressUpdatedNotification(String scriptId, int theProgress) {
    	progress = theProgress;
    	// The double percent below escapes the progress percent sign from
    	// being interpreted as a format char:
    	PigProgressListener.log.info(String.format("Progress %s: %d%%", scriptId, progress));    	
    }
    
    /**
     * Invoked just after all MR jobs spawned by the script are completed.
     * @param scriptId the unique id of the script
     * @param numJobsCompleted the total number of MR jobs succeeded
     */
    public void launchCompletedNotification(String scriptId, int theNumJobsSucceeded) {
    	numJobsCompleted += theNumJobsSucceeded;
    	jobComplete = true;
    	endTime = System.currentTimeMillis();
    	PigProgressListener.log.info(String.format("Launch completed for %s; numJobsCompleted: %d", scriptId, theNumJobsSucceeded));
    }

	@Override
	public void initialPlanNotification(String scriptId, MROperPlan plan) {
		// TODO Auto-generated method stub
		
	}
	
}
