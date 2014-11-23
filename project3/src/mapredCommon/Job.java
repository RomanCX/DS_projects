package mapredCommon;

import java.io.Serializable;
/**
 * Represents a job submitted by user
 * @author RomanC
 * 
 */
public class Job implements Serializable {
	private String inputPath;
	private String outputPath;
	private String jarFile;
	private int jobId;
	private String delim;
	private int numReduceTasks;
	private Class<? extends Mapper> mapClass;
	private Class<? extends Reducer> reduceClass;
	
	/**
	 * Default constructor
	 */
	public Job() {
		
	}
	
	/**
	 * get input path
	 */
	public String getInputPath() {
		return inputPath;
	}
	
	/**
	 * get output path
	 */
	public String getOutputPath() {
		if (!outputPath.endsWith("/")) {
			outputPath += "/";
		}
		return outputPath;
	}
	
	/**
	 * get path of executable jar file
	 */
	public String getJarFile() {
		return jarFile;
	}
	
	/**
	 * 
	 * get job id
	 */
	public int getJobId() {
		return jobId;
	}
	
	/**
	 * 
	 * get characters used to separate key and value both in input file and 
	 * output file
	 */
	public String getDelim() {
		return delim;
	}
	
	/**
	 * 
	 * get number of reduce tasks
	 */
	public int getNumReduceTasks() {
		return numReduceTasks;
	}
	
	/**
	 * 
	 * get mapper class
	 */
	public Class<? extends Mapper> getMapClass() {
		return mapClass;
	}
	
	/**
	 * 
	 * get reducer class
	 */
	public Class<? extends Reducer> getReduceClass() {
		return reduceClass;
	}
	
	/**
	 * 
	 * set job id
	 */
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}
	
	/**
	 * 
	 * set number of reduce tasks
	 */
	public void setNumReduceTasks(int numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}
	
	/**
	 * 
	 * set mapper class
	 */
	public void setMapClass(Class<? extends Mapper> mapClass) {
		this.mapClass = mapClass;
	}
	
	/**
	 * 
	 * set reducer class
	 */
	public void setReduceClass(Class<? extends Reducer> reduceClass) {
		this.reduceClass = reduceClass;
	}
	
	/**
	 * 
	 * set characters used to separate key and value both in input file and 
	 * output file
	 */
	public void setDelim(String delim) {
		this.delim = delim;
	}
	
	/**
	 * 
	 * set path of jar file 
	 */
	public void setJarFile(String jarFile) {
		this.jarFile = jarFile;
	}
	
	/**
	 * 
	 * set input path
	 */
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}
	
	/**
	 * 
	 * set output path
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
}
	
