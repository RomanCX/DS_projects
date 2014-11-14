package mapredCommon;


import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import namenode.DatanodeInfo;

public class MapTask extends Task{
	// input block of this map task
	private int blockId;
	// info of datanode from which map task can fetch its input 
	private DatanodeInfo datanode;
	// user defined mapper function
	private Mapper mapper;
	
	public MapTask(int taskId, int blockId, Job job) {
		super(taskId, job);
		this.blockId = blockId;
	}
	
	public void setDatanode(DatanodeInfo datanode) {
		this.datanode = datanode;
	}
	
	@Override
	public void run(String tmpDir) throws Exception {
		RecordReader reader = new RecordReader(blockId, datanode, job.getDelim());
		RecordWriter writer = new RecordWriter(taskId, tmpDir, job.getDelim());
		String jarFilePath = job.getJarFile();
		mapper = MapReduceUtils.getMapper(jarFilePath);

		/* 
		 * To do: get a mapper
		 */
		while (reader.hasNext()) {
			mapper.map(reader.nextKey(), reader.nextValue(), writer);
		}
	}
	

	
}
