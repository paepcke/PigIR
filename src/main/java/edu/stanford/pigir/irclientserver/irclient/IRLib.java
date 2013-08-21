package edu.stanford.pigir.irclientserver.irclient;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

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
	
	/*---------------------------
	* warcNgrams()
	*-------------------*/
	
	/**
	 * Given a WARC file and an ngram arity, generate ngrams from the WARC file.
	 * The result will be stored in a subdirectory of the WARC file's directory.
	 * That new subdirectoy will be named <warcFile>_ngrams.csv.gz, and it will
	 * contain all the constituent 'part-xxxxx.gz' files. If the given WARC file
	 * is in HDFS, the result will be there as well. If the file is local, so is
	 * the the result subdirectoy.
	 * @param warcFile readable WARC file
	 * @param arity number of words in each ngram
	 * @return a JobHandle_I, which will contain the status of the job
	 */
	public static JobHandle_I warcNgrams(File warcFile, int arity) {
		File destDir = new File(FilenameUtils.getFullPath(warcFile.getAbsolutePath()));
		return warcNgrams(warcFile, 
						  arity, 
						  false,  // no stopword removal
						  2,      // min word len in ngrams,
						  20,     // max word len in ngrams
						  destDir    // no destDir spec  
						  );     
	}
	
	/**
	 * Given a WARC file and an ngram arity, generate ngrams from the WARC file.
	 * The result will be stored in the given directory under the name
	 * <warcFile>_ngrams.csv.gz, and it will contain all the constituent 'part-xxxxx.gz' 
	 * files. If the given WARC file is in HDFS, the result will be there as well. 
	 * If the file is local, so is the result subdirectoy. Callers may ask for
	 * stopwords to be removed (see {@link edu.stanford.pigir.pigudf#isStopword stopwords).
	 * The minimum and maximum length of ngram words can be controlled (default is 2 and 20,
	 * respectively). Words of length outside this range are discarded.
	 * @param warcFile readable WARC file
	 * @param arity number of words in each ngram
	 * @param minlength minimum length of words to be included in ngrams
	 * @param maxlength maximum length of words to be included in ngrams
	 * @param destDir destination directory for result
	 * @return a JobHandle_I, which will contain the status of the job
	 */
	@SuppressWarnings("serial")
	public static JobHandle_I warcNgrams(File warcFile,
										 int arity,
										 boolean removeStopwords,
										 int minLength,
										 int maxLength,
										 File destDir) {
		ServiceResponsePacket resp   = null;
		
		final String warcPath        = warcFile.getAbsolutePath();
		final String theArity        = Integer.toString(arity);
		final String stopwordRemoval = (removeStopwords ? "1" : "0");
		final String theMinLen       = Integer.toString(minLength);
		final String theMaxLen       = Integer.toString(maxLength);
		final String ngramDest       = getDefaultDestName(warcFile, destDir, "ngrams.csv");
		try {
			resp = irclient.sendProcessRequest("warcNgrams", 
												new HashMap<String,String>(){{
															    put("WARC_FILE", warcPath);
																put("ARITY", theArity);
																put("FILTER_STOPWORDS", stopwordRemoval);
																put("WORD_LEN_MIN", theMinLen);
																put("WORD_LEN_MAX", theMaxLen);
															 	put("NGRAM_DEST", ngramDest);
															 	put("USER_CONTRIB", "target/classes");
															 	put("PIGIR_HOME", ".");
															 	put("exectype", IRLib.exectype);
															 	}});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resp.getJobHandle();
	}

	/*---------------------------
	* ngramFrequencies()
	*-------------------*/
	
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

	
	/*---------------------------
	* getProgress()
	*-------------------*/
	
	@SuppressWarnings("serial")
	public static JobHandle_I getProgress(JobHandle_I jobHandle) throws IOException {
		final String jobName = jobHandle.getJobName();
		ServiceResponsePacket resp = irclient.sendProcessRequest("getJobStatus", 
																 new HashMap<String,String>(){{
																	 	put("jobName", jobName);
																	 	}});
		return resp.getJobHandle();
	}

	
	/*---------------------------
	* waitForResult()
	*-------------------*/
	
	/**
	 * Safely wait for a job to finish. The wait occurrs in 
	 * a newly created thread, making waitForResult() reentrant.
	 * If given timeout is zero, wait forever. Else return after at
	 * most timeout milliseconds. This method calls getProgress()
	 * every half second. The method will return when getProgress() indicates
	 * either success or failure.
	 * @param jobHandle the JobHandle_I returned from the job submission method.
	 * @param timeout milliseconds to wait before concluding that something is wrong. Zero: wait forever.
	 * @return the given JobHandle_I with status and message updated.
	 */
	public static JobHandle_I waitForResult(JobHandle_I jobHandle, long timeout) {
		WaitThread waitThread = new WaitThread();
		waitThread.start(jobHandle);
		try {
			waitThread.join(timeout);
			if (waitThread.isAlive()) {
				// timed out:
				waitThread.stopThread();
				jobHandle.setStatus(JobStatus.FAILED);
				jobHandle.setMessage(String.format("Getting progress timed out for job %s",
											       jobHandle.getJobName()));
				return jobHandle;
			}
		}  catch (InterruptedException e) {
			jobHandle.setStatus(JobStatus.FAILED);
			jobHandle.setMessage(String.format("Wait for progress on job %s interrupted: %s",
											   jobHandle.getJobName(), e.getMessage()));
		}
		return jobHandle;
	}

	
	/*---------------------------
	* setExectype()
	*-------------------*/
	
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
	
	
	/*---------------------------
	* setScriptRootDir()
	*-------------------*/
	/**
	 * Set the root directory for Pig scripts. For default and explanation
	 * of meaning, see
	 * {@link  edu.stanford.pigir.irclientserver#IRServConf IRServConf.java}.
	 * @param dir new directory.
	 * @return JobHandle_I that reports success.
	 * @throws IOException
	 */
	public static JobHandle_I setScriptRootDir(String dir) throws IOException {
	
		Map<String,String> params = new HashMap<String,String>();
		params.put("scriptRoot", dir);
		return irclient.sendProcessRequest("setPigScriptRoot", params).getJobHandle();
	}
	
	// --------------------------------  P R I V A T E -------------------

	/**
	 * From the source file name for an IRService operation, construct
	 * the target file path into which the Pig script of the operation will
	 * put the results. Ex:
	 *       getDefaultDestName("/user/john/blue.txt.gz", "ngrams.csv")
	 *    returns:
	 *              /user/john/blue.txt.gz_ngrams.csv.gz  
	 * @param warcPath
	 * @param destDir
	 * @param newSuffix
	 * @return the newly constructed result file where Pig script will place result.
	 */
	@SuppressWarnings("unused")
	private static String getDefaultDestName(File warcPath, String newSuffix) {
		String warcPathStr = warcPath.getAbsolutePath();
		// Directory of given path:
		String dir = FilenameUtils.getFullPath(warcPathStr);
		return getDefaultDestName(warcPath, new File(dir), newSuffix);
	}
	
	/**
	 * From the source file name for an IRService operation, construct
	 * the target file path into which the Pig script of the operation will
	 * put the results. Ex:
	 *       getDefaultDestName("/user/john/blue.txt.gz", "/user/john/results", "ngrams.csv")
	 *    returns:
	 *              /user/john/results/blue.txt.gz_ngrams.csv.gz  
	 * @param warcPath
	 * @param destDir
	 * @param newSuffix
	 * @return the newly constructed result file where Pig script will place result.
	 */
	private static String getDefaultDestName(File warcPath, File destDir, String newSuffix) {

		String warcPathStr = warcPath.getAbsolutePath();
		String dir  = destDir.getAbsolutePath();
		
		// File name of given path:
		String fileName = FilenameUtils.getName(warcPathStr);
		
		String res = new File(dir, fileName + "_" + newSuffix + ".gz").getAbsolutePath(); 
		
		return res;
	}
	
	//---------------------  Wait Thread ------------------------
	
	private static class WaitThread extends Thread {
		
		JobHandle_I jobHandle = null;
		boolean keepRunning = true;
		
		public void start(JobHandle_I theJobHandle) {
			super.start();
			jobHandle = theJobHandle;
		}
			
		public void run() {
			while (keepRunning) {
				try {
					jobHandle = getProgress(jobHandle);
				} catch (IOException e) {
					jobHandle.setStatus(JobStatus.FAILED);
					jobHandle.setMessage(e.getMessage());
					return;
				}
				if ((jobHandle.getStatus() == JobStatus.SUCCEEDED) ||
					(jobHandle.getStatus() == JobStatus.FAILED)) {
					return;
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					continue;
				}
			}
		}
		
		public void stopThread() {
			keepRunning = false;
		}
	}
}
