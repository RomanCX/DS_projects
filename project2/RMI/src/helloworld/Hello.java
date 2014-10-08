package helloworld;

import registry.LocateRegistry;
import registry.Registry;
import remote.RemoteException;

public class Hello implements HelloInterface
{
  private static final String serverName = "hello";

  public Hello() throws RemoteException
  {
  }

  // This is actually the starting point that registers the object
  public static void main (String []args)
  {
    try
    {
      // Instantiate an instance of this class -- create a Hello object
      Hello server = new Hello();

      Registry reg = LocateRegistry.getRegistry();
      // Tie the name "Hello" to the Hello object we just created
      reg.bind(serverName, server);

      // Just a console message
      System.out.println ("Hello Server ready");
    }
    catch (Exception e)
    {
      // Bad things happen to good people
      e.printStackTrace();
    }
  }

  // This is the one real method
  public String sayHello(String name) throws RemoteException
  {
    return "Hello World! Hello " + name; 
  }
}