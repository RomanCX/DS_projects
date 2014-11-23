package mapredCommon;

/**
 * Mapper is an individual task that receives set of key-value pairs as its input and 
 * transform them into intermediate key-value pairs.
 * <p> The intermediate key-value pairs will be sent to {@link Reducer} to do reducing.
 *
 * @author RomanC
 *
 */
public abstract class Mapper {
	
	/**
	 *  Setup will be executed once in the beginning of a map task.
	 *  <p> Users can override this method to do preparations for this task
	 */
	public void setup() {
		//Does nothing by default
	}
	
	/**
	 * Maps a single key-value pair to an intermediate key-value pair
	 * @param key  input key
	 * @param value input value
	 * @param writer facility to output the intermediate key-value pair to local disk
	 */
	public abstract void map(int key, String value, RecordWriter writer);
}
