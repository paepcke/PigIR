package pigir;

import java.io.IOException;
import java.util.UUID;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class GetUUID extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		return UUID.randomUUID().toString(); 
		
	}
}
