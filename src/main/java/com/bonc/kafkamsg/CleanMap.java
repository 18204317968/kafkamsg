package com.bonc.kafkamsg;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

public class CleanMap extends Thread {
	public void run() {
		int i=0;
		for(;;){
			try {
				Thread.sleep(30*60*1000);
				ArrayList<String> list=new ArrayList<String>();
				Map<String,Object> map=CreateHDFS.hdfsMap.get("history");
				synchronized (map){
					if (null != map&&map.size()>0) {
						for (Entry<String, Object> m : map.entrySet()) {
							if (null != m) {
								Date before = (Date)m.getValue();
								Date now =new Date();
								long l=now.getTime()-before.getTime();
								if(l>Resource.HISTRYTIME*60*1000){
									list.add(m.getKey());
								}
							}
						}
					}
				}
				if(null!=list&&list.size()>0){
					System.out.println("Clean   !!!");
					for(String s:list){
						map.remove(s);
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			++i;
			System.out.println("clean check !!!"+i);
		}
	}
}
