package edu.stanford.pigir.irclientserver.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import edu.stanford.pigir.irclientserver.Utils;

public class TestGetLastPigAlias {

	@Test
	public void test() throws IOException {
		String alias = Utils.getPigResultAlias("src/test/resources/pigScriptForAliasFinding1.pig");
		assertEquals("myAlias", alias);
		
		alias = Utils.getPigResultAlias("src/test/resources/pigScriptForAliasFinding2.pig");
		assertEquals("rightAlias", alias);
		
		alias = Utils.getPigResultAlias("src/test/resources/pigScriptForAliasFinding3.pig");
		assertEquals("rightAlias", alias);
		
		alias = Utils.getPigResultAlias("src/test/resources/pigScriptForAliasFinding4.pig");
		assertEquals(null, alias);
	}
}
