package protocals;

import java.net.InetAddress;
import java.rmi.Remote;
import java.util.HashMap;
import java.util.List;

import namenode.DatanodeInfo;

public interface ClientProtocal extends Remote{
	/*
	 * Returns hashmap<datanodeId, list of Block Ids>
	 */
	public HashMap<DatanodeInfo, List<Integer> > read(String fileName, String address);
	/*
	 * Returns hashmap<block id, list of datanode info>
	 */
	public HashMap<Integer, List<DatanodeInfo> >write(String fileName, int splitNum);
}
