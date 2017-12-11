package com.bonc.kafkamsg;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

public class KafkaMain 
{
    public static void main( String[] args )
    {

		 System.out.println("kafka.............start");
		 KafkaMain kfk=new KafkaMain();
		 kfk.doShutdownHook();
		 KafkaConsumer consumerThread1 = new KafkaConsumer("news");
		 KafkaConsumer consumerThread2 = new KafkaConsumer("soc");
		 consumerThread1.start();
		 consumerThread2.start();
    }
    private void doShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					if (null != CreateHDFS.hdfsMap&&CreateHDFS.hdfsMap.size()>0) {
						ArrayList<String> list=new ArrayList<String>();
						for (Entry<String, Map<String, Object>> m : CreateHDFS.hdfsMap.entrySet()) {
							if (null != m) {
								list.add(m.getKey());
							}
						}
						if(null!=list&&list.size()>0){
							for(String s:list){
								CreateHDFS.hdfsMap.remove(s);
							}
						}
						CreateHDFS.fileSystem.close();
					}
					System.out.println("end");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
}
