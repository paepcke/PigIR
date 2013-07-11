package edu.stanford.pigir.pigudf.unittests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.pigudf.ConcatColumns;

public class TestConcatColumns {

	Tuple arg = null;
	Tuple toFuse = null;
	
	@Before
	public void setUp() throws Exception {
		// New args and tuple to fuse for each test:
		arg = new DefaultTuple();
		arg.append(null);
		arg.append(null);
		arg.append(null);
		toFuse = new DefaultTuple();
	}

	@Test 
	public void testEmptyArgs() throws IOException {
		DefaultTuple emptyArg = new DefaultTuple();
		// Pass empty argument tuple: error
		assertEquals(null, new ConcatColumns().exec(emptyArg));
	}

	@Test(expected = IOException.class) 
	public void testWrongArgTypes() throws IOException {
		// Bad type for slice spec:
		arg.set(ConcatColumns.SLICE_SPEC_POS, 10);
		// Pass non-string as first argument tuple: error
		new ConcatColumns().exec(arg);
	}
	
	@Test(expected = IOException.class) 
	public void testBadSliceDefs() throws IOException {
		toFuse.append("foo");
		arg.set(ConcatColumns.SLICE_SPEC_POS, "3:1");
		arg.set(ConcatColumns.CONCAT_SEPARATOR_POS, "");
		arg.set(ConcatColumns.TUPLE_TO_FUSE_POS, toFuse);
		// Start > end:
		new ConcatColumns().exec(arg);
	}
	
	@Test
	public void testGoodArgs() throws IOException {
		toFuse.append("notIncluded");
		toFuse.append("foo");
		toFuse.append("bar");
		// Fuse cols 1 to end of tuple:
		arg.set(ConcatColumns.SLICE_SPEC_POS, "1:4");
		arg.set(ConcatColumns.TUPLE_TO_FUSE_POS, toFuse); 
		String fusedStr = new ConcatColumns().exec(arg);
		assertEquals("foobar", fusedStr);

		arg.set(ConcatColumns.SLICE_SPEC_POS, "1:1");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("foo", fusedStr);
		
		arg.set(ConcatColumns.SLICE_SPEC_POS, ":1");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("notIncluded", fusedStr);
		
		arg.set(ConcatColumns.SLICE_SPEC_POS, "0:");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("notIncludedfoobar", fusedStr);
		
		arg.set(ConcatColumns.SLICE_SPEC_POS, ":");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("notIncludedfoobar", fusedStr);
		
		arg.set(ConcatColumns.SLICE_SPEC_POS, "1:-1");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("foo", fusedStr);
		
		arg.set(ConcatColumns.SLICE_SPEC_POS, ":-1");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("notIncludedfoo", fusedStr);
		
		arg.set(ConcatColumns.SLICE_SPEC_POS, ":-2");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("notIncluded", fusedStr);

		arg.set(ConcatColumns.SLICE_SPEC_POS, ":-3");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("notIncluded", fusedStr);
	
		arg.set(ConcatColumns.SLICE_SPEC_POS, "1:2");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("foo", fusedStr);
		
		arg.set(ConcatColumns.SLICE_SPEC_POS, "1:3");
		arg.set(ConcatColumns.CONCAT_SEPARATOR_POS, "|");
		fusedStr = new ConcatColumns().exec(arg);
		assertEquals("foo|bar", fusedStr);
		
	}
}
