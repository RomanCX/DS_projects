package protocals;

import java.net.InetAddress;
import java.rmi.Remote;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import namenode.DatanodeInfo;

public interface ClientProtocal extends Remote{
	/*
	 * Returns hashmap<block id, datanode info>
	 */
	public HashMap<Integer, DatanodeInfo > read(String fileName, String address);
	/*
	 * Returns hashmap<block id, list of datanode info>
	 */
	public TreeMap<Integer, List<DatanodeInfo> >write(String fileName, int splitNum);
	
	/*
	 *  Get the size of block
	 */
	public int getBlockSize();
}
