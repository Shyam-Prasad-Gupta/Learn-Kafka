package com.shyam.practice;

import java.util.HashMap;
import java.util.Map;

public class Test {
	
	public static void main(String args[]) {
		
		Map<Integer, String> hashMap = new HashMap<Integer, String>();
		
		hashMap.put(1, "shyam");
		hashMap.put(2, "prasad");
		hashMap.put(1, "gupta");
		System.out.println(hashMap);
		
		
		
	}

}
