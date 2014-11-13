package mapredCommon;

public interface Mapper {
	public void map(String key, String value, RecordWriter writer);
}
