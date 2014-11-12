import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import protocals.NamenodeProtocal;


public class TestImp implements Test {

	public static void main(String[] args) {
		try {
			TestImp test = new TestImp();
			Test testStub = (Test)UnicastRemoteObject.exportObject(test, 0);

			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind("test", testStub);
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void kao(Class clazz) {
		try {
			Object o = clazz.newInstance();
			java.lang.reflect.Method method;
			method = o.getClass().getMethod("hello");
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
