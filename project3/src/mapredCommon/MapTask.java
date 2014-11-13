package mapredCommon;


import namenode.DatanodeInfo;

public class MapTask extends Task{
	// input block of this map task
	private int blockId;
	// info of datanode from which map task can fetch its input 
	private DatanodeInfo datanode;
	// characters used to separate key and value
	private String delim;
	// a temporal directory to store the output file of map task
	private String tmpDir;
	// user defined mapper function
	private Mapper mapper;
	//To do, may need more variables
	
	public MapTask(int id, String jarFileName, int blockId,
				DatanodeInfo datanode) {
		super(id, jarFileName);
		this.blockId = blockId;
		this.datanode = datanode;
	}
	
	public void run() {
		RecordReader reader = new RecordReader(blockId, datanode, delim);
		RecordWriter writer = new RecordWriter(id, tmpDir, delim);
		/* 
		 * To do: get a mapper
		 */
		while (reader.hasNext()) {
			mapper.map(reader.nextKey(), reader.nextValue(), writer);
		}
	}
}
