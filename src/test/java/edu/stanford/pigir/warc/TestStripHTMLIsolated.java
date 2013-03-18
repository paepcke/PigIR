package edu.stanford.pigir.warc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.pigudf.StripHTML;

public class TestStripHTMLIsolated {

	StripHTML stripper = new StripHTML();
	
	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void test() throws IOException {
    	String htmlStr = "<head><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>";
    	Tuple input = TupleFactory.getInstance().newTuple(1);
    	input.set(0, new DataByteArray(htmlStr.getBytes()));
    	Tuple res;

    	res = stripper.exec(input);
    	//System.out.println(res);
    	//System.out.println(stripper.outputSchema(Common.getTupleSchema(input)));
    	//System.out.println(stripper.getArgToFuncMapping());
    	assertEquals("This is bold and a link anchor", ((DataByteArray)res.get(0)).toString());
    	assertEquals((Integer)30, ((Integer)res.get(1)));
	}
}
