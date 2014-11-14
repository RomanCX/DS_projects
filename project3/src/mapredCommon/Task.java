package mapredCommon;

import java.io.Serializable;

public class Task implements Serializable {
	int taskId;
	String tmpDir;
	Job job;
	
	public Task(int taskId, String tmpDir, Job job) {
		this.taskId = taskId;
		this.job = job;
		this.tmpDir = tmpDir;
	}
}
