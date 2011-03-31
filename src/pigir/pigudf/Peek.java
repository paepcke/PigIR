package pigir.pigudf;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import pigir.Common;

public class Peek extends EvalFunc<String> {
	protected final Logger logger = Logger.getLogger(getClass());
	
	public String exec(Tuple input) throws IOException {
		
		logger.info("Whoami: " + Common.runUnixCommand("whoami"));
		return "foo";
	}
}
