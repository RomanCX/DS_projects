package mapredCommon;

import java.util.Iterator;

/**
 * Reducer is an individual task that reduce values with same key to a 
 * smaller set of values.
 * @author RomanC
 *
 */
public abstract class Reducer {
	
	/**
	 * Setup will be executed once in the beginning of a reduce task.
	 * <p>Users can override this method to do preparations for reduce task
	 */
	public void setup() {
		//Does nothing by default
	}
	
	/**
	 * Reduce values with same key to smaller set of values
	 * @param key input key
	 * @param values input values with the same key
	 * @param writer facility to write the output key-value pairs to dfs
	 */
	public abstract void reduce(String key, Iterator<String> values, RecordWriter writer);
}
