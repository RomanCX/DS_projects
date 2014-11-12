
import java.io.File;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.URL;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ClassTest {
	public static void main(String[] args) {
		String fileName = args[0];
		String address = args[1];
		int port = Integer.parseInt(args[2]);
		try {
			Class myClass = Class.forName(fileName);
			System.out.println("..............");
			Registry reg = LocateRegistry.getRegistry(address, port);
			Test test = (Test)reg.lookup("test");
			test.kao(myClass);
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*String jarFileName = args[0];
		try {
			File file = new File(jarFileName);
			URL url = file.toURL();
			
			JarFile jar = new JarFile(jarFileName);
			Enumeration<JarEntry> entries = jar.entries();
			while (entries.hasMoreElements()) {
				JarEntry entry = entries.nextElement();
				String entryName = entry.getName();
				if (entryName.endsWith(".class")) {
					Class classToLoad = Class.forName(entryName);
					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("hehe");
		}*/
	}
}
