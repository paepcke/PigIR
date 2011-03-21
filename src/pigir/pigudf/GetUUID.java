package pigir.pigudf;

import java.io.IOException;
import java.util.UUID;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Pig UDF for obtaining a universally unique identifier.
 * May also be used by other Java functions, notably other
 * UDFs.
 * 
 * @author paepcke
 *
 */
public class GetUUID extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		return makeUUID();
		
	}
	
	/**
	 * Obtain a universally unique identifier. 
	 * 
	 * @return UUID string
	 */
	public static String newUUID() {
		return makeUUID();
	}
	
	/*
	 * The sausage is made here.
	 * @return UUID as string
	 */
	private static String makeUUID() {
		return UUID.randomUUID().toString(); 
	}
}
