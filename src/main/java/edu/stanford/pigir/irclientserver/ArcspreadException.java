package edu.stanford.pigir.irclientserver;

import org.json.JSONException;
import org.json.JSONStringer;

import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

/**
 * @author paepcke
 *
 *  Exceptions passed around across the client server boundary.
 *  All these exceptions inherit from ArcspreadException, which
 *  extends Java exceptions. The exceptions are classes nested
 *  inside ArcspreadException. 
 *  
 *  What's special is that these exceptions implement JobHandle_I,
 *  and can therefore be passed wherever computation results are
 *  passed across the client/server boundary. The methods required
 *  by this interface are inherited from AcrspreadException.
 */

@SuppressWarnings("serial")
public abstract class ArcspreadException extends Exception {

	private static String DEFAULT_JOBNAME = "<unnamed>";
	protected String jobName = ArcspreadException.DEFAULT_JOBNAME;
	// errorCode gets overwritten by subclasses:
	protected static int errorCode = 0;
	// cause gets overwritten by subclasses:
	protected static String cause = "";
	
	public ArcspreadException() {
		super();
	}

	public ArcspreadException(String msg) {
		super(msg);
	}

	public ArcspreadException(String msg, Throwable cause) {
		super(msg,cause);
	}
	
	public ArcspreadException(String theJobName, String msg) {
		super(msg);
		jobName = theJobName;
	}
	
	public int getProgress() {
		throw new RuntimeException(new NotImplementedException());
	}
	
	public int getNumJobsRunning() {
		throw new RuntimeException(new NotImplementedException());
	}

	public long getRuntime() {
		throw new RuntimeException(new NotImplementedException());
	}

	public long getBytesWritten() {
		throw new RuntimeException(new NotImplementedException());
	}

	public void setProgress(int progress) {
		throw new RuntimeException(new NotImplementedException());
	}
	
	public void setNumJobsRunning(int numJobsRunning) {
		throw new RuntimeException(new NotImplementedException());
	}

	public void setRuntime(long runtime) {
		throw new RuntimeException(new NotImplementedException());
	}

	public void setBytesWritten(long bytesWritten) {
		throw new RuntimeException(new NotImplementedException());
	}
	
	
	public JSONStringer toJSON(JSONStringer stringer) {
		try {
			stringer.key("jobName");
			stringer.value(jobName);
			stringer.key("errorCode");
			stringer.value(Integer.toString(errorCode));
			stringer.key("exceptionCause");
			stringer.value(cause);
			stringer.key("message");
			String msg = getMessage();
			// Make sure there is *something* in the message
			// fld. If no msg was provided, use the 'cause,' which
			// every subclass is sure to set.
			if (msg == null)
				stringer.value(cause);
			else
				stringer.value(msg);
			stringer.key("status");
			stringer.value(JobStatus.FAILED.toJSONValue());
		} catch (JSONException e) {
			throw new RuntimeException(String.format("Cannot convert %s to JSON: %s", this.getClass().getName(), e.getMessage()));
		}
		return stringer;
	}
		
	// -----------------------------  Each Exception is a Subclass ----------------------------------
	
	/*--------------------------
	 * NotImplementedException
	 * -------------------------*/
	
	public static class NotImplementedException extends ArcspreadException implements JobHandle_I {
		{errorCode = 1;}
		{cause = "Not Implemented";}
	
		
		public NotImplementedException(String msg) {
			super(msg);
		}
		
		public NotImplementedException(String msg, Throwable cause) {
			super(msg,cause);
		}
		
		public NotImplementedException(String theJobName, String msg) {
			super(msg);
			jobName = theJobName;
		}
		
		public NotImplementedException() {
			super();
		}
	}

	/*--------------------------
	 * CommIOException
	 * -------------------------*/
	public static class CommIOException extends ArcspreadException implements JobHandle_I {
	
		{errorCode = 2;}
		{cause = "IO Exception During Communication";}
		
		public CommIOException(String msg) {
			super(msg);
		}
		
		public CommIOException(String theJobName, String msg) {
			super(msg);
			jobName = theJobName;
		}
		
		public CommIOException() {
			super();
		}
	}

	/*--------------------------
	 * ParameterException
	 * -------------------------*/
	
	public static class ParameterException extends ArcspreadException implements JobHandle_I {
	
		{errorCode = 3;}
		{cause = "Bad or Missing Parameter";}
		
		public ParameterException(String msg) {
			super(msg);
		}
		
		public ParameterException(String theJobName, String msg) {
			super(msg);
			jobName = theJobName;
		}
		
		public ParameterException() {
			super();
		}
	}
	
	/*--------------------------
	 * PreHadoopFailure
	 * -------------------------*/
	
	public static class PreHadoopException extends ArcspreadException implements JobHandle_I {
	
		{errorCode = 4;}
		{cause = "Pig could not launch Hadoop";}
		
		public PreHadoopException(String msg) {
			super(msg);
		}
		
		public PreHadoopException(String theJobName, String msg) {
			super(msg);
			jobName = theJobName;
		}
		
		public PreHadoopException() {
			super();
		}
	}
	
	// ----------------------------  Methods Inherited By all Exception Subclasses Above ---------
	public int getErrorCode() {
		int res = -1;
		try {
			res = (Integer) this.getClass().getDeclaredField("errorCode").get(this);
		} catch (NoSuchFieldException | SecurityException e) {
			// We guarantee that the field is defined (in each exception subclass)
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public JobStatus getStatus() {
		return JobHandle_I.JobStatus.FAILED;
	}

	public String getJobName() {
		return jobName;
	}
	
	public void setStatus(JobStatus newStatus) {}

	public void setErrorCode(int newErrorCode) {}

	public void setMessage(String newMessage) {}
	
	// -----------------------  Testing  ------------------------------
	
	
	
	public static void main(String[] args) {
		NotImplementedException exc = new NotImplementedException("Test msg");
		System.out.println(String.format("Msg: '%s'. Errcode: %d", exc.getMessage(), NotImplementedException.errorCode));
		
		exc = new NotImplementedException("Job test msg", "myJob");
		System.out.println(String.format("Msg: '%s'. Errcode: %d. Job: %s", exc.getMessage(), exc.getErrorCode(), exc.getJobName()));
		
		exc = new NotImplementedException();
		System.out.println(String.format("Msg: '%s'. Errcode: %d. Job: %s", exc.getMessage(), exc.getErrorCode(), exc.getJobName()));
		
	}
}

