package edu.stanford.pigir.pigudf.unittests;

import java.io.IOException;

import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.pigudf.ConcatColumns;

public class TestConcatColumns {

	@Before
	public void setUp() throws Exception {
	}

	@Test(expected = IOException.class)
	public void test() throws IOException {
		Tuple arg = new DefaultTuple();
		new ConcatColumns().exec(arg);
	}

}
