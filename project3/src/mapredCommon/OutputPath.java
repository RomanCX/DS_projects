package mapredCommon;

import java.io.Serializable;

public class OutputPath implements Serializable {
	Job job;
	int taskId;
	int reduceId;
	
	public OutputPath(Job job, int taskId, int reduceId) {
		this.job = job;
		this.taskId = taskId;
		this.reduceId = reduceId;
	}
	
	public String getMapPath(String outputDir, int taskTrackerId) {
		return outputDir + "/map" + job.getJobId() + "_T" 
				+ taskId + "_R" + reduceId + "_TK" + taskTrackerId;
	}
	
	String getReduceSortedPath(String outputDir) {
		return outputDir + "/reduce_tmp" + job.getJobId() + "_" + taskId;

	}
	
	public String getReduceTmpPath(String outputDir, int taskTrackerIndex, int outputIndex) {
		return getReduceSortedPath(outputDir) + "_" + taskTrackerIndex + "_" + outputIndex;
	}
	
	public String getReducePath(String outputDir) {
		return outputDir + "/reduce" + job.getJobId() + "_" + taskId;
	}
	
	public Job getJob() {
		return job;
	}
	
	public int getReduceId() {
		return reduceId;
	}
}

