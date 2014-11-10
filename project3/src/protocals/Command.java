package protocals;

import java.util.ArrayList;

public class Command {
	public enum Operation {
		FETCH_DATA,
		DELETE_DATA,
		READ_DATA,
		WRITE_DATA
	}
	
	public Operation operation;
	private ArrayList<Host> targets;
	private ArrayList<Integer> blockIds;
	
	public Command() {
		
	}
	
	public ArrayList<Host> getTargets() {
		return targets;
	}
	
	public ArrayList<Integer> getBlockIds() {
		return blockIds;
	}
}
