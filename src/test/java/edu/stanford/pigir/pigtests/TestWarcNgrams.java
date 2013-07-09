package edu.stanford.pigir.pigtests;

import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import edu.stanford.pigir.Common;

public class TestWarcNgrams {

	static PigServer pserver;
	static Properties props = new Properties();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	try {
		props.setProperty("pig.usenewlogicalplan", "false");
		//pserver = new PigServer(ExecType.MAPREDUCE, props);
		pserver = new PigServer(ExecType.LOCAL, props);
		URI piggybankPath = new File("target/classes/piggybank.jar").toURI();
		pserver.registerJar(piggybankPath.toString());
		String pigirJarPath  = Common.findVersionedFileName("target", "pigir", "jar");
		pserver.registerJar(pigirJarPath);
		
	} catch (ExecException e) {
		e.printStackTrace();
	}
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	@Ignore
	public void test() {
		fail("Not yet implemented");
	}

}
