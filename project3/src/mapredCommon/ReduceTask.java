package mapredCommon;

public class ReduceTask extends Task{

	String outputFileName;
	public ReduceTask(int id, String jarFileName, String outputFileName) {
		super(id, jarFileName);
		this.outputFileName = outputFileName;
	}

}
