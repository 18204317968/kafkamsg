package com.bonc.kafkamsg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import com.teamsun.kafka.m001.Resource;  

import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread {

	private final ConsumerConnector consumer;
	private String topic;

	public static CreateHDFS createHDFS = new CreateHDFS();

	public KafkaConsumer(String mt) {

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());

		if (mt.equals("news")) {
			this.topic = Resource.TOPICNEWS;
		} else if (mt.equals("soc")) {
			this.topic = Resource.TOPICSOC;
		}
	}

	private static ConsumerConfig createConsumerConfig() {

		Properties props = new Properties();
		props.put("zookeeper.connect", Resource.ZKCONNECT);
		props.put("group.id", Resource.GROUPID);
		props.put("zookeeper.session.timeout.ms", "5000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("zookeeper.connection.timeout.ms", "10000");
		props.put("rebalance.backoff.ms", "2000");
		props.put("rebalance.max.retries", "10");
//		props.put("auto.offset.reset", "largest");
		return new ConsumerConfig(props);
	}

	private static void doHDFS(String msg, String mt) throws Exception {
		if(null!=mt&&mt.equals("news")){
			createHDFS.doNews(msg);
		}else{
			createHDFS.doSoc(msg);
		}
	}

	@Override
	public void run() {
		System.out.println(topic);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		CleanMap clean=new CleanMap();//定时清除hdfsmap中存入的以写文件
		clean.start();
		while (it.hasNext()) {
			String msg = new String(it.next().message());
//			System.out.println(msg);
			try {
				if (topic.equals(Resource.TOPICNEWS)) {
					doHDFS(msg, "news");//写入新闻数据
				} else {
					doHDFS(msg, "soc");//写入社交数据
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
