package helloworld;

import registry.*;

public class HelloClient {
	private static String server = "localhost";
	private static String helloServerName = "hello";
	
	public static void main(String[] args) {
		try {
			Registry reg = LocateRegistry.getRegistry(server);
			HelloInterface hello = (HelloInterface)reg.lookup(helloServerName);
			
			String theGreeting = hello.sayHello(args[0]);
			System.out.println(theGreeting);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
