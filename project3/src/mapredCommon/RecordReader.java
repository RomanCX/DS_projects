package mapredCommon;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.StringTokenizer;

import protocals.Command;
import protocals.Operation;
import datanode.Block;
import namenode.DatanodeInfo;

public class RecordReader {
	private ArrayList<String> tokens;
	private String delim;
	private int idx;
	
	public RecordReader(int blockId, DatanodeInfo datanode, String delim) throws Exception {
		Socket socket = new Socket(datanode.getAddress(), datanode.getPort());
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		oos.writeObject(new Command(Operation.READ_DATA, blockId, null));
		oos.flush();
		Block block = (Block)ois.readObject();
		socket.close();
		oos.close();
		ois.close();
		
		tokens = new ArrayList<String>();
		StringTokenizer st = new StringTokenizer(block.getData(), "\n");
		while (st.hasMoreTokens()) {
			tokens.add(st.nextToken());
		}
		this.delim = delim;
		this.idx = 0;
		System.out.println(tokens);
	}
	
	public boolean hasNext() {
		return idx < tokens.size();
	}
	
	public int nextKey() {
		//int pos = tokens.get(idx).indexOf(delim);
		//return tokens.get(idx).substring(0, pos);
		return idx;
	}
	
	public String nextValue() {
		/*
		int pos = tokens.get(idx).indexOf(delim);
		String value = tokens.get(idx).substring(pos + delim.length());
		idx++;
		return value;
		*/
		String returnValue = tokens.get(idx);
		idx++;
		return returnValue;
	}
}
