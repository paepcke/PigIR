package edu.stanford.pigir.irclientserver.irserver;

import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;

import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.irclient.IRClient;

public class TestIRServer {

	@BeforeClass
	public static void setUpOnce() throws Exception {
		Log.set(Log.LEVEL_DEBUG);
		IRClient irClient = new IRClient(); 
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}

}
