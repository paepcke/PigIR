package edu.stanford.pigir.irclientserver.irclient;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.IRServConf.HADOOP_EXECTYPE;
import edu.stanford.pigir.irclientserver.JobHandle_I;

public class TestIRLib {

	@Before
	public void setUp() throws Exception {
		IRLib.setExectype(HADOOP_EXECTYPE.LOCAL);
	}

	@Test
	public void testWarcNgrams() throws IOException {
		JobHandle_I jobHandle = IRLib.warcNgrams(new File("src/test/resources/tinyWarc1_0.warc"), 3);
		System.out.println(jobHandle.getJobName());
		jobHandle = IRLib.getProgress(jobHandle);
		System.out.println(String.format("Jobname: %s (%s): %s", jobHandle.getJobName(), jobHandle.getStatus(), jobHandle.getMessage()));
	}

}
