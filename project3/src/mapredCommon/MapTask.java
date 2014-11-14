package mapredCommon;


import namenode.DatanodeInfo;

public class MapTask extends Task{
	// input block of this map task
	private int blockId;
	// info of datanode from which map task can fetch its input 
	private DatanodeInfo datanode;
	// user defined mapper function
	private Mapper mapper;
	
	public MapTask(int taskId, String tmpDir, int blockId, DatanodeInfo datanode,
				Job job) {
		super(taskId, tmpDir, job);
		this.blockId = blockId;
		this.datanode = datanode;
	}
	
	public void run() throws Exception {
		RecordReader reader = new RecordReader(blockId, datanode, job.getDelim());
		RecordWriter writer = new RecordWriter(taskId, tmpDir, job.getDelim());
		String jarFileName = job.getJarFile();
		
		/* 
		 * To do: get a mapper
		 */
		while (reader.hasNext()) {
			mapper.map(reader.nextKey(), reader.nextValue(), writer);
		}
	}
}
