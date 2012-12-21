package edu.stanford.pigir.pigudf;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import edu.stanford.pigir.Common;

public class Peek extends EvalFunc<Tuple> {
	protected final Logger logger = Logger.getLogger(getClass());
	
	public Tuple exec(Tuple input) throws IOException {
		
		logger.info("Whoami: " + Common.runUnixCommand("whoami"));
		logger.info("Input tuple: '" + input + "'");
		return input;
	}
}
