package mapredCommon;

import java.io.IOException;

/**
 * RecordWriter is a facility that writes key-value pairs to a file system
 * @author RomanC
 *
 */
public interface RecordWriter {
	/**
	 * Write a key-value pair
	 * @throws IOException
	 */
	public void write(String key, String value)throws IOException;
}
