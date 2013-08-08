package edu.stanford.pigir;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class TestCommon {

	@Test
	public void testFileViaClassPath() throws IOException {
		File file = Common.fileViaClasspath("edu/stanford/pigir/Common.class");
		assertTrue(file.getAbsolutePath().endsWith("target/classes/edu/stanford/pigir/Common.class"));
		file = Common.fileViaClasspath("warcStripHTML.pig");
		assertTrue(file.getAbsolutePath().endsWith("target/classes/warcStripHTML.pig"));
	}
}
