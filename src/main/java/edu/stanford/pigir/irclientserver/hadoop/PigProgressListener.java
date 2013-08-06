/**
 * 
 */
package edu.stanford.pigir.irclientserver.hadoop;

import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;

/**
 * @author paepcke
 *
 * Every Pig script is started with an instance of this
 * listener. This instance allows for monitoring of progress
 * over the wire.
 * 
 */
public class PigProgressListener implements org.apache.pig.tools.pigstats.PigProgressNotificationListener {

	String scriptID = "";
	int progress = 0;
	int numJobsSucceeded = 0;
	
	int numOfJobsToLaunch = 0;
	int numOfJobsSubmitted = 0;
	String assignedJobId = "";
	JobStats finishedJobStats = null;
	JobStats failedJobStats = null;
	OutputStats outputStats = null;

	
    /**
     * Invoked just before launching MR jobs spawned by the script.
     * @param scriptId the unique id of the script
     * @param numJobsToLaunch the total number of MR jobs spawned by the script
     */
    public void launchStartedNotification(String scriptId, int theNumJobsToLaunch) {
    	numOfJobsToLaunch = theNumJobsToLaunch;
    }
    
    /**
     * Invoked just before submitting a batch of MR jobs.
     * @param scriptId the unique id of the script
     * @param numJobsSubmitted the number of MR jobs in the batch
     */
    public void jobsSubmittedNotification(String scriptId, int theNumJobsSubmitted) {
    	numOfJobsSubmitted = theNumJobsSubmitted;
    }
    
    /**
     * Invoked after a MR job is started.
     * @param scriptId the unique id of the script 
     * @param assignedJobId the MR job id
     */
    public void jobStartedNotification(String scriptId, String theAssignedJobId) {
    	assignedJobId = theAssignedJobId;
    }
    
    /**
     * Invoked just after a MR job is completed successfully. 
     * @param scriptId the unique id of the script 
     * @param jobStats the {@link JobStats} object associated with the MR job
     */
    public void jobFinishedNotification(String scriptId, JobStats theFinishedJobStats) {
    	finishedJobStats = theFinishedJobStats;
    }
    
    /**
     * Invoked when a MR job fails.
     * @param scriptId the unique id of the script 
     * @param jobStats the {@link JobStats} object associated with the MR job
     */
    public void jobFailedNotification(String scriptId, JobStats theFailedJobStats) {
    	failedJobStats = theFailedJobStats;
    }
    
    /**
     * Invoked just after an output is successfully written.
     * @param scriptId the unique id of the script
     * @param outputStats the {@link OutputStats} object associated with the output
     */
    public void outputCompletedNotification(String scriptId, OutputStats theOutputStats) {
    	outputStats = theOutputStats;
    }
    
    /**
     * Invoked to update the execution progress. 
     * @param scriptId the unique id of the script
     * @param progress the percentage of the execution progress
     */
    public void progressUpdatedNotification(String scriptId, int theProgress) {
    	progress = theProgress;
    }
    
    /**
     * Invoked just after all MR jobs spawned by the script are completed.
     * @param scriptId the unique id of the script
     * @param numJobsSucceeded the total number of MR jobs succeeded
     */
    public void launchCompletedNotification(String scriptId, int theNumJobsSucceeded) {
    	numJobsSucceeded = theNumJobsSucceeded;
    }
	
}
