package mapredCommon;

import java.io.Serializable;

public abstract class Task implements Serializable {
	int taskId;
	Job job;
	String tmpDir;
	
	public Task(int taskId, Job job) {
		this.taskId = taskId;
		this.job = job;
	}
	
	public void setTmpDir(String tmpDir) {
		this.tmpDir = tmpDir;
	}
	
	public int getTaskId() {
		return taskId;
	}
	public abstract void run() throws Exception;
}
