package transactionalIO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;

public class TransactionalFileInputStream extends InputStream 
				implements Serializable {
	private String name;
	private long offset;
	private RandomAccessFile f;
	
	public TransactionalFileInputStream(String name) throws FileNotFoundException {
		try {
			f = new RandomAccessFile(name, "r");
		} catch (FileNotFoundException e) {
			throw e;
		}
		this.name = name;
		offset = 0;
	}
	
	@Override
	public int read() throws IOException {
		int ret;
		
		ret = f.read();
		offset += 1;
		return ret;
	}
	
	@Override
	public int read(byte[] b) throws IOException{		
		int ret;
		
		ret = f.read(b);
		offset += ret;
		return ret;
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {		
		int ret;
		
		ret = f.read(b, off, len);
		offset += ret;
		return ret;
	}
	
	@Override
	public void close() throws IOException {
		f.close();
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		f.close();
		f = null;
		oos.writeObject(name);
		oos.writeLong(offset);
		oos.writeObject(f);
	}
	
	private void readObject(ObjectInputStream ois)
			throws IOException, ClassNotFoundException {
		name = (String)ois.readObject();
		offset = ois.readLong();
		f = (RandomAccessFile)ois.readObject();
		f = new RandomAccessFile(name, "r");
		f.seek(offset);
	}
}
