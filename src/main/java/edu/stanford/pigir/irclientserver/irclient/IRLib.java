/**
 * 
 */
package edu.stanford.pigir.irclientserver.irclient;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServConf;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

/**
 * @author paepcke
 *
 */
public class IRLib {

	private static IRClient irclient = IRClient.getInstance();
	private static String exectype = null;
	{if (IRServConf.hadoopExecType == IRServConf.HADOOP_EXECTYPE.LOCAL) 
		exectype = "local";
	 else
		exectype = "mapreduce";
	}

	// ------------------------  Public  Methods --------------------
/*                           \t\t\t [{-s | --stopwords}] (default: no stopword removal) \n
                           \t\t\t [{-i | --minlength}] (default: 2)\n
                           \t\t\t [{-a | --maxlength}] (default: 20)\n
                           \t\t\t [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; else '/user/<username>') \n
                           \t\t\t <warcFilePathOnHDFS> \n
                           \t\t\t <arity> \n
*/
	
	public static JobHandle_I warcNgrams(File warcFile, int arity) {
		return warcNgrams(warcFile, 
						  arity, 
						  false,  // no stopword removal
						  2,      // min word len in ngrams,
						  20);     // max word len in ngrams
						  
	}
	
	@SuppressWarnings("serial")
	public static JobHandle_I warcNgrams(File warcFile,
										 int arity,
										 boolean removeStopwords,
										 int minLength,
										 int maxLength,
										 File destDir) {
		ServiceResponsePacket resp   = null;
		final String filePath        = warcFile.getAbsolutePath();
		final String theArity        = Integer.toString(arity);
		final String stopwordRemoval = (removeStopwords ? "1" : "0");
		final String theMinLen       = Integer.toString(minLength);
		final String theMaxLen       = Integer.toString(maxLength);
		final String destDirPath     = destDir.getAbsolutePath();
		try {
			resp = irclient.sendProcessRequest("warcNgrams", 
												new HashMap<String,String>(){{
													            put("NGRAM_FILE", filePath);
																put("ARITY", theArity);
																put("FILTER_STOPWORDS", stopwordRemoval);
																put("WORD_LEN_MIN", theMinLen);
																put("WORD_LEN_MAX", theMaxLen);
															 	put("DEST_DIR", destDirPath);
															 	put("exectype", IRLib.exectype);
															 	}});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resp.getJobHandle();
	}

	/**
	 *   Given an ngram file or a directory of compressed
	 *   or non-compressed ngram files, produce a new file
	 *   of two columns: frequency, and frequency-of-frequency.
	 *   This output is useful for GoodTuring smoothing, and 
	 *   feeds right into the respective program at http://www.grsampson.net/D_SGT.c
	 *
	 *   The output file will be a series of lines separated by newline
	 *   characters, where all lines contain two positive integers:
	 *   one of the ngram frequencies of the input file, followed by the 
	 *   frequency of that frequency. Separator is comma.
	 *   The lines will be in ascending order of frequency.
	 *
	 *   Note: supposedly, using the code cited above, the first frequency
	 *   	 in the file is to be 1. Not sure that's truly a must. But 
	 *	 the underlying Pig script will not assure this condition.
	 *
	 *   The resulting .csv file will be ${DEST_DIR}/${NGRAM_NAME}_freqs.gz
 
	 * @param ngramCSVFile file containing lines of ngrams, the constituent words separated by commas.
	 * 	                   For local Hadoop the file should be on the local file system, else on HDFS.
	 * @param destDir is the target directory. 
	 * 	      For local Hadoop the file should be on the local file system, else on HDFS.
	 * @return a JobHandle_I containing the result.
	 * 
	 * FREQS_DEST    destination of output: directory if source is a directory, else dest file name.
	 */
	@SuppressWarnings("serial")
	public static JobHandle_I ngramFrequencies(File ngramCSVFile, File destDir) {
		ServiceResponsePacket resp = null;
		final String filePath    = ngramCSVFile.getAbsolutePath();
		final String destDirPath = destDir.getAbsolutePath();
		try {
			resp = irclient.sendProcessRequest("ngramFrequencies", 
												new HashMap<String,String>(){{
															 	put("NGRAM_FILE", filePath);
															 	put("DEST_DIR", destDirPath);
															 	put("exectype", IRLib.exectype);
															 	}});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resp.getJobHandle();
	}
	
	@SuppressWarnings("serial")
	public static JobHandle_I getProgress(JobHandle_I jobHandle) throws IOException {
		final String jobName = jobHandle.getJobName();
		ServiceResponsePacket resp = irclient.sendProcessRequest("getJobStatus", 
																 new HashMap<String,String>(){{
																	 	put("jobName", jobName);
																	 	}});
		return resp.getJobHandle();
	}
	
	/**
	 * Set the Hadoop execution type for upcoming calls. Choices
	 * are HADOOP_EXECTYPE.LOCAL and HADOOP_EXECTYPE.MAPREDUCE.
	 * NOTE: this setting only affects upcoming jobs, not ones that
	 * are already running. 
	 * @param newExectype LOCAL or MAPREDUCE
	 */
		
	public static void setExectype(IRServConf.HADOOP_EXECTYPE newExectype) {
		IRServConf.hadoopExecType = newExectype;
		if (newExectype == IRServConf.HADOOP_EXECTYPE.LOCAL) 
			IRLib.exectype = "local";
		else
			IRLib.exectype = "mapreduce";
	}
	
	
	public static JobHandle_I setScriptRootDir(String dir) throws IOException {
	
		Map<String,String> params = new HashMap<String,String>();
		params.put("scriptRoot", dir);
		return irclient.sendProcessRequest("setPigScriptRoot", params).getJobHandle();
	}
	
	
	/**
	 * Busy-wait for a job to either succeed, fail, or be declared dead.
	 * NOTE: consider using getResultQueue().**** instead
	 * @param jobHandle
	 * @return
	 * @throws IOException 
	 */
	public JobHandle_I awaitResultPolling(JobHandle_I jobHandle) throws IOException {
		return awaitResultPolling(jobHandle, IRServConf.STARTUP_TIME_MAX);
	}
	
	/**
	 * Busy-wait for a job to either succeed, fail, or be declared dead.
	 * NOTE: consider using getResultQueue().**** instead
	 * @param jobHandle
	 * @param timeout
	 * @return
	 * @throws IOException 
	 */
	public JobHandle_I awaitResultPolling(JobHandle_I jobHandle, long timeout) throws IOException {
		long startWaitTime = System.currentTimeMillis();
		while (true) {
			if ((System.currentTimeMillis() - startWaitTime) > timeout) {
				jobHandle.setStatus(JobStatus.FAILED);
				jobHandle.setMessage(String.format("Job '%s' did not either fail or succeed within timeout of %d seconds", jobHandle.getJobName(),timeout/1000));
				return jobHandle;
			}
			jobHandle = getProgress(jobHandle);
			if (jobHandle.getStatus() == JobStatus.SUCCEEDED)
				return jobHandle;
			if (jobHandle.getStatus() == JobStatus.FAILED) {
				String errMsg = String.format("Job '%s' returned failure (error code %d): %s", jobHandle.getJobName(), jobHandle.getErrorCode(), jobHandle.getMessage());
				jobHandle.setMessage(errMsg);
				return jobHandle;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				continue;
			}
		}
	}
	
	// --------------------------------  P R I V A T E -------------------

}
