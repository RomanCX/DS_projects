package helloworld;


import registry.LocateRegistry;
import registry.Registry;



public class HelloClient {
	private static String server = "localhost";
	private static String helloServerName = "hello";
	
	public static void main(String[] args) {
		try {
			Registry reg = LocateRegistry.getRegistry(server);
			String[] remoteObjectList = reg.list();
			for (String remoteObjectName : remoteObjectList) {
				System.out.println(remoteObjectName);
			}
			HelloInterface hello = (HelloInterface)reg.lookup(helloServerName);
			
			String theGreeting = hello.sayHello("hahahaha");
			System.out.println(theGreeting);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
