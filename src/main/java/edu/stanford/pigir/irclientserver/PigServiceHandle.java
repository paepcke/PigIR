package edu.stanford.pigir.irclientserver;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

public class PigServiceHandle implements JobHandle_I {

	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		
	String	  jobName 	= "null";
	JobStatus status 	= JobStatus.UNKNOWN;

	int	      progress  = 0;
	int       numJobsRunning = 0;
	long 	  runtime = 0;
	long 	  bytesWritten = 0;
	int       errorCode = -1;
	String    message   = "";
	
	/**
	 * Used to remember the JSON parsing state when
	 * a parsing error occurs. See fromJson():
	 */
	private enum ServiceHandleJSONDecodeState {
		GET_JOBNAME,
		GET_JOB_STATUS,
		GET_PROGRESS,
		GET_NUM_JOBS_RUNNING,
		GET_RUNTIME,
		GET_BYTES_WRITTEN,
		GET_ERROR_CODE,
		GET_MESSAGE,
	}
	
	public PigServiceHandle() {
		// Kryonet deserialization needs a no-args constructor. 
	}
	
	public PigServiceHandle(String theJobName, JobStatus theStatus) {
		status = theStatus;
		jobName = theJobName;
	}
	
	public PigServiceHandle(String theJobName, JobStatus theStatus, String errorMessage) {
		this(theJobName, theStatus);
		message = errorMessage;
	}

	public int getProgress() {
		return progress;
	}

	public void setProgress(int progress) {
		this.progress = progress;
	}

	public int getNumJobsRunning() {
		return numJobsRunning;
	}

	public void setNumJobsRunning(int numJobsRunning) {
		this.numJobsRunning = numJobsRunning;
	}

	public long getRuntime() {
		return runtime;
	}

	public void setRuntime(long runtime) {
		this.runtime = runtime;
	}

	public long getBytesWritten() {
		return bytesWritten;
	}

	public void setBytesWritten(long bytesWritten) {
		this.bytesWritten = bytesWritten;
	}
	
	@Override
	public JobStatus getStatus() {
		return status;
	}
	
	@Override
	public String getJobName() {
		return jobName;
	}

	@Override
	public int getErrorCode() {
		return errorCode;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public void setStatus(JobStatus newStatus) {
		status = newStatus;
	}

	@Override
	public void setErrorCode(int newErrorCode) {
		errorCode = newErrorCode;
	}

	@Override
	public void setMessage(String newMessage) {
		message = newMessage;
	}
	
	public String toString() {
		if (getJobName() != null)
			return String.format("<PigServiceHandle:%s (status: %s)>", getJobName(), getStatus());
		else
			return String.format("<PigServiceHandle:unnamed (status: %s)>", getStatus());
	}
	
	public static String getPigTimestamp() {
		return PigServiceHandle.timestampFormat.format(new Date()); 
	}
	
	public JSONStringer toJSON(JSONStringer stringer) throws JSONException {
		stringer.key("jobName");
		stringer.value(getJobName());
		stringer.key("status");
		stringer.value(getStatus().toJSONValue());
		stringer.key("progress");
		stringer.value(getProgress());
		stringer.key("numJobsRunning");
		stringer.value(getNumJobsRunning());
		stringer.key("runtime");
		stringer.value(getRuntime());
		stringer.key("errorCode");
		stringer.value(getErrorCode());
		stringer.key("message");
		stringer.value(getMessage());
		
		return stringer;
	}
	
	public static PigServiceHandle fromJSON(String jsonStr) throws JSONException {
		String jobName = null;
		JobStatus status = null;
		int progress = 0;
		int numJobsRunning = -1;
		long runtime = 0;
		long bytesWritten = 0;
		int errorCode = -1;
		String msg = null;
		PigServiceHandle res = null;
		// Before each parsing step, set the var
		// decodeState, which indicates what we are
		// about to try and get out of the JSON obj. 
		// That way we can provide good error messages below: 
		ServiceHandleJSONDecodeState decodeState = ServiceHandleJSONDecodeState.GET_JOBNAME; 

		try {
			JSONObject jObj = new JSONObject(jsonStr);
			jobName = jObj.getString("jobName");
			
			decodeState = ServiceHandleJSONDecodeState.GET_PROGRESS;
			progress = jObj.getInt("progress");

			decodeState = ServiceHandleJSONDecodeState.GET_NUM_JOBS_RUNNING;
			numJobsRunning = jObj.getInt("numJobsRunning");
			
			decodeState = ServiceHandleJSONDecodeState.GET_RUNTIME;
			runtime = jObj.getLong("runtime");
			
			decodeState = ServiceHandleJSONDecodeState.GET_BYTES_WRITTEN;
			bytesWritten = jObj.getLong("bytesWritten");
			
			decodeState = ServiceHandleJSONDecodeState.GET_ERROR_CODE;
			errorCode = jObj.getInt("errorCode");
			
			decodeState = ServiceHandleJSONDecodeState.GET_MESSAGE;
			msg = jObj.getString("message");
			
			decodeState = ServiceHandleJSONDecodeState.GET_JOB_STATUS;
			status = JobStatus.fromJSONValue(jObj.getString("status"));			
			
			res = new PigServiceHandle(jobName, status, msg);
			res.setProgress(progress);
			res.setNumJobsRunning(numJobsRunning);
			res.setRuntime(runtime);
			res.setBytesWritten(bytesWritten);
			res.setErrorCode(errorCode);
			return res;
		} catch (JSONException e) {
			String errMsg = null;
			switch (decodeState) {
			case GET_JOBNAME:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching the job name from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_PROGRESS:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching progress info from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_NUM_JOBS_RUNNING:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching number of running jobs from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_RUNTIME:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching runtime from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_BYTES_WRITTEN:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching number of bytes written from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_ERROR_CODE:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching the error code from '%s' (%s)", jsonStr, e.getMessage());
			case GET_MESSAGE:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching the message from '%s' (%s)", jsonStr,  e.getMessage());
			case GET_JOB_STATUS:
				errMsg = String.format("Cannot decode JSON object job handle: failed while extracting the job status from '%s' (%s)", jsonStr,  e.getMessage());
			}
			throw new JSONException(errMsg);
		}
	}
	
}
