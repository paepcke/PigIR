package edu.stanford.pigir.nohadoop;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class TestTokenizerRegExp {

	TokenizeRegExp tokenizer;
	
	@Before
	public void setUp() throws Exception {
		tokenizer = new TokenizeRegExp();
	}

	@Test
	public void test() throws IOException {
		
		String strArg;
		strArg = "On a sunny day";
		assertEquals(Arrays.asList("On", "a", "sunny", "day"), tokenizer.tokenize(strArg));

		strArg = "Testing it!";
		assertEquals(Arrays.asList("Testing", "it"),  tokenizer.tokenize(strArg));

		strArg = "FDA";
		assertEquals(Arrays.asList("FDA"),tokenizer.tokenize(strArg));

		//-------------
		//System.out.println("*****Now with whitespace as regexp:");
		String regexp = "[\\s]";

		strArg = "On a sunny day";
		assertEquals(Arrays.asList("On", "a", "sunny","day"),tokenizer.tokenize(strArg, regexp));

		strArg = "Testing it!";
		assertEquals(Arrays.asList("Testing", "it!"),  tokenizer.tokenize(strArg, regexp));

		strArg = "FDA";
		assertEquals(Arrays.asList("FDA"),tokenizer.tokenize(strArg, regexp));

		//-------------
		//System.out.println("*****Test stopword elimination:");
		strArg = "foo";
		assertEquals(Arrays.asList("foo") ,tokenizer.tokenize(strArg, TokenizeRegExp.KILL_STOPWORDS));

		strArg = "This is a stopword test.";
		assertEquals(Arrays.asList("stopword", "test"),tokenizer.tokenize(strArg, TokenizeRegExp.KILL_STOPWORDS));
		//-------------
		//System.out.println("*****Test url preservation :");
		// use standard regexp
		// no stopword elimination
		// want URL preservation 

		strArg = "foo";
		assertEquals(Arrays.asList("foo") ,tokenizer.tokenize(strArg, TokenizeRegExp.PRESERVE_STOPWORDS, TokenizeRegExp.PRESERVE_URLS));

		strArg = "http://infolab.stanford.edu";
		assertEquals(Arrays.asList("http://infolab.stanford.edu"),tokenizer.tokenize(strArg, TokenizeRegExp.PRESERVE_STOPWORDS, TokenizeRegExp.PRESERVE_URLS));

		strArg = "And now url (embedded http://infolab.stanford.edu) text";
		assertEquals(Arrays.asList("And","now","url", "embedded", "http://infolab.stanford.edu", "text"), 
					 tokenizer.tokenize(strArg, TokenizeRegExp.PRESERVE_STOPWORDS, TokenizeRegExp.PRESERVE_URLS));
		
		strArg = "The word http text.";
		assertEquals(Arrays.asList("The", "word", "http", "text"), tokenizer.tokenize(strArg, TokenizeRegExp.PRESERVE_STOPWORDS, TokenizeRegExp.PRESERVE_URLS));
		
		strArg = "Finally, (file://C:/Users/kennedy/.baschrc) two URLs. ftp://blue.mountain.com/?parm1=foo&parm2=bar";
		assertEquals(Arrays.asList("Finally","file://C:/Users/kennedy/.baschrc","two","URLs","ftp://blue.mountain.com/?parm1=foo&parm2=bar"),
								   tokenizer.tokenize(strArg, TokenizeRegExp.PRESERVE_STOPWORDS, TokenizeRegExp.PRESERVE_URLS));
		
		//-------------
		//System.out.println("*****Now with 'fo.*o' as regexp:");
		regexp = "fo.*o";

		strArg = "foo";
		assertEquals(Arrays.asList(),tokenizer.tokenize(strArg, regexp));

		strArg = "fobaro";
		assertEquals(Arrays.asList(),tokenizer.tokenize(strArg, regexp));
		
		strArg = "fobarotree";
		assertEquals(Arrays.asList("tree"), tokenizer.tokenize(strArg, regexp));

		strArg = "fo is your papa barotree";
		assertEquals(Arrays.asList("tree"), tokenizer.tokenize(strArg, regexp));

		strArg = "fo is your papa barotree and with you.";
		assertEquals(Arrays.asList("u."), tokenizer.tokenize(strArg, regexp));
		

		//-------------
		//System.out.println("*****Pulling out URLs:");

		assertEquals(TokenizeRegExp.findURL("This is http://foo.bar.com/blue.html", 8), "http://foo.bar.com/blue.html");

		assertEquals(TokenizeRegExp.findURL("file://me.you.her/blue.html", 0), "file://me.you.her/blue.html");

		assertEquals(TokenizeRegExp.findURL("URL is ftp://me.you.her/blue.html, and embedded.", 7), "ftp://me.you.her/blue.html");

		assertEquals(TokenizeRegExp.findURL("No index given ftp://me.you.her/blue.html, and embedded."), "ftp://me.you.her/blue.html");

		assertEquals(TokenizeRegExp.findURL("file://me.you.her/blue.html without index"), "file://me.you.her/blue.html"); 

		System.out.println("Passed all tests.");
	}
}


