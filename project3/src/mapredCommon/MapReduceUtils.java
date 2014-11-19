package mapredCommon;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class MapReduceUtils {
	public static ClassLoader getClassLoader(String jarFilePath) throws MalformedURLException {
		File file = new File(jarFilePath);
		@SuppressWarnings("deprecation")
		URL url = file.toURL();
		URL[] urls = new URL[]{url};
		ClassLoader cl = new URLClassLoader(urls);
		return cl;
	}
	
	@SuppressWarnings("unchecked")
	public static Object getClassObject(String jarFilePath, Class superClass) throws IOException, 
		ClassNotFoundException, InstantiationException, IllegalAccessException {
		ClassLoader cl = getClassLoader(jarFilePath);
		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> entries = jarFile.entries();
		
		Object returnValue = null;
		while (entries.hasMoreElements()) {
			JarEntry entry = entries.nextElement();
			String classFileName = entry.getName();
			int pos = classFileName.indexOf(".class");
			if (pos == -1) {
				continue;
			}
			String className = classFileName.substring(0, pos).replace('/', '.');
			Class<?> clazz = cl.loadClass(className);
			if (superClass.isAssignableFrom(clazz)) {
				returnValue = clazz.newInstance();
			}
		}
		jarFile.close();
		return returnValue;
	}
	
	public static Mapper getMapper(String jarFilePath) throws IOException, 
		ClassNotFoundException, InstantiationException, IllegalAccessException {
		return (Mapper) getClassObject(jarFilePath, Mapper.class);
	}
	
	public static Reducer getReducer(String jarFilePath) throws IOException, 
	ClassNotFoundException, InstantiationException, IllegalAccessException {
		return (Reducer) getClassObject(jarFilePath, Reducer.class);
	}
	
}
