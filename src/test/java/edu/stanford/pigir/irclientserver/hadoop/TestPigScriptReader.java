package edu.stanford.pigir.irclientserver.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;

import org.junit.Test;

public class TestPigScriptReader {

	PigScriptReader reader = null;

	@Test
	public void testOneCommentLine() throws FileNotFoundException {
		reader = new PigScriptReader("src/test/resources/pigReadTest1.pig");
		assertTrue(reader.hasNext());
		String singleComment = reader.next();
		assertEquals("", singleComment);
	}
	
	@Test
	public void testOneCodeLine() throws FileNotFoundException {
		reader = new PigScriptReader("src/test/resources/pigReadTest2.pig");
		assertTrue(reader.hasNext());
		String singleCodeLine= reader.next();
		assertEquals("lines = LOAD 'foo.txt' USING PigStorage() AS(line:chararray);", singleCodeLine);
		assertFalse(reader.hasNext());
	}
	
	@Test
	public void testCodeAndCommentsMixed() throws FileNotFoundException {
		reader = new PigScriptReader("src/test/resources/pigReadTest3.pig");
		assertTrue(reader.hasNext());
		String singleCodeLine= reader.next();
		assertEquals("lines = LOAD 'foo.txt' USING PigStorage() AS(line:chararray);", singleCodeLine.trim());
		assertFalse(reader.hasNext());
	}
	
	@Test
	public void testMultilineCode() throws FileNotFoundException {
		reader = new PigScriptReader("src/test/resources/pigReadTest4.pig");
		assertTrue(reader.hasNext());
		String singleCodeLine= reader.next();
		assertEquals("lines = LOAD 'foo.txt' USING PigStorage() AS(line:chararray);", singleCodeLine.trim());
		assertFalse(reader.hasNext());
	}
	
	@Test
	public void testBlockCommentOnly() throws FileNotFoundException {
		reader = new PigScriptReader("src/test/resources/pigReadTest5.pig");
		assertTrue(reader.hasNext());
		String singleCodeLine= reader.next();
		assertEquals("", singleCodeLine);
		assertFalse(reader.hasNext());
	}

	@Test
	public void testBlockCommentPlusCodeDiffLine() throws FileNotFoundException {
		reader = new PigScriptReader("src/test/resources/pigReadTest6.pig");
		assertTrue(reader.hasNext());
		String singleCodeLine = reader.next();
		assertEquals("STORE foo;", singleCodeLine);
		assertFalse(reader.hasNext());
	}

	@Test
	public void testBlockCommentPlusCodeSameLine() throws FileNotFoundException {
		reader = new PigScriptReader("src/test/resources/pigReadTest7.pig");
		assertTrue(reader.hasNext());
		String singleCodeLine = reader.next();
		assertEquals("STORE foo;", singleCodeLine);
		assertFalse(reader.hasNext());
	}

	@Test
	public void testBlockCommentPlusCodeSameLineReversed() throws FileNotFoundException {
		reader = new PigScriptReader("src/test/resources/pigReadTest8.pig");
		assertTrue(reader.hasNext());
		String singleCodeLine = reader.next();
		assertEquals("STORE foo;", singleCodeLine);
		assertFalse(reader.hasNext());
	}

}
