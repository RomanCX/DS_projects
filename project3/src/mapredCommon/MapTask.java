package mapredCommon;


import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import tasktracker.TaskTracker;
import namenode.DatanodeInfo;

public class MapTask extends Task {
	// input block of this map task
	private int blockId;
	// info of datanode from which map task can fetch its input 
	private DatanodeInfo datanode;
	// user defined mapper function
	
	public MapTask(int taskId, int blockId, Job job) {
		super(taskId, job);
		this.blockId = blockId;
	}
	
	public void setDatanode(DatanodeInfo datanode) {
		this.datanode = datanode;
	}
	
	@Override
	public void run() {
		RecordReader reader;
		try {
			reader = new RecordReader(blockId, datanode, job.getDelim());
			MapRecordWriter writer = new MapRecordWriter(job, taskId, tmpDir);
			Mapper mapper;
			mapper = MapReduceUtils.getMapper(job);
			mapper.setup();
			while (reader.hasNext()) {
				mapper.map(reader.nextKey(), reader.nextValue(), writer);
			}
			TaskTracker.getInstance().reportFinished(taskId);
			writer.writeAll();
		} catch (Exception e1) {
			e1.printStackTrace();
			TaskTracker.getInstance().reportFailed(taskId);
		}


	}
	

	
}
