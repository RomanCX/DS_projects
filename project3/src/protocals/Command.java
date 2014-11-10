package protocals;

import java.util.ArrayList;

public class Command {
	
	public Operation operation;
	private ArrayList<Host> targets;
	private ArrayList<Integer> blockIds;
	
	public Command(Operation operation, ArrayList<Integer> blockIds) {
		this.operation = operation;
		this.blockIds = blockIds;
	}
	
	public ArrayList<Host> getTargets() {
		return targets;
	}
	
	public ArrayList<Integer> getBlockIds() {
		return blockIds;
	}
}
