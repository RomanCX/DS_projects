package protocals;

import java.rmi.Remote;
import java.util.Map;


public interface DatanodeProtocal extends Remote {
	public DnRegistration register(String address, int port);
	public Command heartBeat(int nodeId, Map<Integer, String> blocks);
}
