package mapredCommon;

public interface Mapper {
	public void map(int key, String value, RecordWriter writer);
}
