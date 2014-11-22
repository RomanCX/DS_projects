package mapredCommon;

public abstract class Mapper {
	
	public void setup() {
		//Does nothing by default
	}
	
	public abstract void map(int key, String value, RecordWriter writer);
}
