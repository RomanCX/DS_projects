package mapredCommon;

import java.util.List;

public class MapTask extends Task{
	public MapTask(int id, String jarFileName, List<Integer> inputBlockIds) {
		super(id, jarFileName);
		this.inputBlodkIds = inputBlockIds;
	}

	List<Integer> inputBlodkIds;
	
}
