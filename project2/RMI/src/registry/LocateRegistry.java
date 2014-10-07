package registry;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class LocateRegistry {
	//Disable the default constructor.
	private LocateRegistry() {};
	
	public static Registry getRegistry(String host) throws UnknownHostException {
		if (host == null) {
			return new RegistryServer_Stub(Registry.REGISTRY_OBJECT_NAME, 
					InetAddress.getLocalHost().getHostAddress(), Registry.REGISTRY_PORT);
		} else {
			return new RegistryClient_Stub(Registry.REGISTRY_OBJECT_NAME, 
					host, Registry.REGISTRY_PORT);
		}
	}
	
	public static Registry getRegistry()throws UnknownHostException {
		return getRegistry(null);
	}
	
	public static Registry createRegistry() {
		return new RegistryImp();
	}
}
