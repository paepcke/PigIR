package pigir.pigudf;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

/**
 * Pig UDF for obtaining an identifier that is unique
 * within the current job. This function is useful for 
 * tuple ID generation.
 * 
 * A counter is incremented every time Pig calls this 
 * function. The LUID is constructed by turning that
 * counter into a string, and prepending a concatenation
 * of the job number, the task number, the task attempt
 * number, and one underscore.
 * 
 * @author paepcke
 *
 */

public class GetLUID extends EvalFunc<String> {
	
	private static String idPrefix = null;
	private static Integer prevID = -1;

	@Override
	public String exec(Tuple input) throws IOException {
		return makeUUID();
	}
	
	/*
	 * The sausage is made here.
	 * @return Locally Unique ID as string
	 */
	private String makeUUID() {
		String taskAttemptId = null;
		if (idPrefix == null) {
			// Get this task's unique ID. Has form "attempt_local_0001_m_000000_0".
			// The 0001 is the job id, the 000000 is the task id, and the 0 is
			// the attempt id. Those three together make this execution environment
			// unique.
			Configuration conf = UDFContext.getUDFContext().getJobConf();
			taskAttemptId = conf.get("mapred.task.id");
			TaskAttemptID id = TaskAttemptID.forName(taskAttemptId);
			Integer jobNum = id.getJobID().getId();
			Integer taskNum = id.getTaskID().getId();
			Integer attemptNum = id.getId();
			idPrefix = jobNum.toString() + taskNum.toString() + attemptNum.toString() + "_";
		}
		return idPrefix + (++prevID).toString() ;
	}
}

