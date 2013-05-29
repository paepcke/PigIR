package edu.stanford.pigir;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class TestTextualHTMLLinksRemover {

	@Test
	public void testStr() {
		assertEquals("Test", TextualHTMLLinksRemover.removeLinks("Test"));
		assertEquals("This is a test.", TextualHTMLLinksRemover.removeLinks("This is a test."));
		assertEquals("", TextualHTMLLinksRemover.removeLinks("http://infolab.stanford.edu"));
		assertEquals("My  link.", TextualHTMLLinksRemover.removeLinks("My http://infolab.stanford.edu link."));
		assertEquals(" link.", TextualHTMLLinksRemover.removeLinks("http://infolab.stanford.edu link."));
		assertEquals("My ", TextualHTMLLinksRemover.removeLinks("My http://infolab.stanford.edu"));
	}

	/**
	 * Tests file containing:
	 * 	Test
		This is a test.
		http://infolab.stanford.edu
		My http://infolab.stanford.edu link.
		http://infolab.stanford.edu link.
		My http://infolab.stanford.edu.
	 * @throws IOException 
 
	 */

	@Test
	public void testFile() throws IOException {
		File inFile = new File("src/test/resources/htmlRemovalTest.txt");
		File outFile = File.createTempFile("linkRemovalTest","");
		outFile.deleteOnExit();
		String expected = "Test\n" +
				          "This is a test.\n" +
				          "\n" +
				          "My  link.\n" + 
				          " link.\n" +
				          "My .\n";
		TextualHTMLLinksRemover.removeLinks(inFile, outFile);
		String res = FileUtils.readFileToString(outFile, "UTF-8");
		assertEquals(expected, res);
	}
}
