package mapredCommon;

import java.io.Serializable;

public class Job implements Serializable {
	private String inputPath;
	private String outputPath;
	private String mapperPath;
	private String reducerPath;
	private int jobId;
	
	public Job(String inputPath, String outputPath, String mapperPath,
			String reducerPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.mapperPath = mapperPath;
		this.reducerPath = reducerPath;
	}
	
	public String getInputPath() {
		return inputPath;
	}
	
	public String getOutputPath() {
		return outputPath;
	}
	
	public String mapperPath() {
		return mapperPath;
	}
	
	public String reducerPath() {
		return reducerPath;
	}
	
	public int getJobId() {
		return jobId;
	}
	
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}
}
