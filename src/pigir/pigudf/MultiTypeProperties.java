package pigir.pigudf;

import java.io.Serializable;
import java.util.Properties;

/**
 * A property list in the Java sense, except that values
 * may be primitive types other than just strings. Double,
 * Float, int, and String are allowed.
 *  
 * @author paepcke
 *
 */
public class MultiTypeProperties extends Properties implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Properties props;
	
	public MultiTypeProperties() {
		props = new Properties();
	}
	
	public MultiTypeProperties(Properties initProps) {
		props = new Properties(initProps);
	}

	public String getProperty(String key) {
		return (props.getProperty(key));
	}
	
	public String getProperty(String key, String defaultValue) {
		return props.getProperty(key, defaultValue);
	}
	
	public int getInt(String key, int defaultValue) {
		try {
			return new Integer(props.getProperty(key));
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public float getFloat(String key, float defaultValue) {
		try {
			return new Float(props.getProperty(key));
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public double getDouble(String key, double defaultValue) {
		try {
			return new Double (props.getProperty(key));
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public Object setProperty(String key, String value) {
		return props.setProperty(key, value);
	}
	
	public void setInt(String key, int value) {
		props.setProperty(key, new Integer(value).toString());
	}
	
	public void setFloat(String key, float value) {
		props.setProperty(key, new Float(value).toString());
	}
	
	public void setDouble(String key, double value) {
		props.setProperty(key, new Double(value).toString());
	}
	
	public String toString() {
		return props.toString();
	}
}
