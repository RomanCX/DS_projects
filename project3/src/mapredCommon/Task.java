package mapredCommon;

import java.io.Serializable;

public abstract class Task implements Serializable {
	int taskId;
	Job job;
	
	public Task(int taskId, Job job) {
		this.taskId = taskId;
		this.job = job;
	}
	
	public abstract void run(String tmpDir) throws Exception;
}
