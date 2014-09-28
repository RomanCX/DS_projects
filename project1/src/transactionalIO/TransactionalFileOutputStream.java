package transactionalIO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;

public class TransactionalFileOutputStream extends OutputStream implements Serializable {
	private String name;
	private long offset;
	private RandomAccessFile f;
	
	public TransactionalFileOutputStream(String name, boolean append) throws FileNotFoundException {
		f = new RandomAccessFile(name, "rw");
		this.name = name;
		try {
			if (append == true) {
				offset = f.length();
			}
			else {
				f.setLength(0);
				offset = 0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		f.write(b);
		offset += b.length;
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		f.write(b, off, len);
		offset += len;
	}
	
	@Override
	public void write(int b) throws IOException {
		f.write(b);
		offset += 1;
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
		f = new RandomAccessFile(name, "rw");
		f.seek(offset);
	}
}