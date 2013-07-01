package edu.stanford.pigir;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class TestCommon {

	@Test
	public void testFileViaClassPath() throws IOException {
		//File file = Common.fileViaClasspath(this, "/edu/stanford/pigir/Common.java");
		//File file = Common.fileViaClasspath(this, "/edu/stanford/pigir/Common");
		File file = Common.fileViaClasspath("edu/stanford/pigir/Common.class");
		//File file = Common.fileViaClasspath("Common.class");
		//File file = Common.fileViaClasspath(this, "/Common.java");
		//File file = Common.fileViaClasspath(this, "Common");
		System.out.println(file.getAbsolutePath());
	}

}
