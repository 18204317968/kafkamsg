package com.bonc.kafkamsg;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class TimeOut extends Thread {
	private String mt;
	private String fileDir;
	private String specialId;
	public TimeOut(String mt,String fileDir,String specialId){
		this.mt=mt;
		this.fileDir=fileDir;
		this.specialId=specialId;
	}
	
	/*删除文件*/
	public void deleteFile(Map<String, Object> dataMap){
		try{
			FSDataOutputStream outPutStream=(FSDataOutputStream)dataMap.get("outputStream");
			if(null!=outPutStream){//先关闭输出流
				outPutStream.flush();
				outPutStream.close();
				System.out.println("streamClose...");
			}
			Map<String, Object> historyMap=(Map<String, Object>)CreateHDFS.hdfsMap.get("history");
			historyMap.put(fileDir, new Date());//记录文件已经写过
			String name=(String)dataMap.get("names");//取到所有文件名
			String[] names=name.split("&&");//拆开装入数组中
			if(null!=names&&names.length>0){
				for(String n:names){
					CreateHDFS.fileSystem.delete(new Path(n),true);//执行删除
				}
			}
			System.out.println("delete...");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*写日志*/
	public void writeLog(String fileName,String specialId,String mt) {
		try {
			CreateHDFS.localLog(mt, specialId, 1);//将失败信息同步到hdfs中
			CreateHDFS.HDFSLog(fileName, mt, specialId, 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public void run(){
		Map<String, Object> map=CreateHDFS.hdfsMap.get(mt);
		ArrayList<String> list=new ArrayList<String>();
		int i=0;
		while (null != map.get(fileDir)) {//在文件还在读写阶段执行
			Map<String, Object> dataMap=(Map<String, Object>) map.get(fileDir);
			synchronized (map) {
				dataMap=(Map<String, Object>)map.get(fileDir);
				if (null!=dataMap&&null != dataMap.get("date")) {
					Date before = (Date) dataMap.get("date");
					Date now = new Date();
					long l = now.getTime() - before.getTime();
					if (l > Resource.TIMEOUT * 1000) {//判断间隔时间是否超时
						deleteFile(dataMap);//删除已经写入的文件
						list.add(fileDir);
						writeLog(fileDir,specialId,mt);//记录日志
						System.out.println("Time Out !!!");
						break;
					}
				}
			}
			++i;
			System.out.println("time out check !!!"+i+"  "+mt);
			try {
				Thread.sleep(20*1000);//每20秒检查一次
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if(null!=list&&list.size()>0){
			map.remove(fileDir);//移除map中的filename
			System.out.println("mapRemove...");
		}
	}
}
