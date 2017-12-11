package com.bonc.kafkamsg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.impl.client.DefaultHttpClient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.bonc.kafkamsg.CountryUtil;
import com.bonc.kafkamsg.Resource;

public class CreateHDFS {

	public static Map<String,Map<String,Object>> hdfsMap=new Hashtable<String,Map<String,Object>>();
	public static FileSystem fileSystem=null;
	CountryUtil util=new CountryUtil();
	static{
		hdfsMap.put("history", new Hashtable<String, Object>());
		hdfsMap.put("news", new Hashtable<String, Object>());
		hdfsMap.put("soc", new Hashtable<String, Object>());
		Configuration conf=new Configuration();//取得hdfs连接
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.defaultFS", Resource.FSDEFAULTNAME);
		conf.set("dfs.nameservices", Resource.CLUSTERNAME);
		conf.set("dfs.ha.namenodes."+Resource.CLUSTERNAME, "nn1,nn2");
		conf.set("dfs.namenode.rpc-address."+Resource.CLUSTERNAME+".nn1", Resource.NN1);
		conf.set("dfs.namenode.rpc-address."+Resource.CLUSTERNAME+".nn2", Resource.NN2);
		conf.set("dfs.client.failover.proxy.provider."+Resource.CLUSTERNAME,org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider.class.getName());
		try {
			fileSystem=FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getNewsData(Map<String, Object> map) throws Exception{
		String data="";
		Integer  v_int;
		JSONArray	v_ja;
		String specialId=(String)map.get("specialId");
		
		String 	abstractZh		=(String)map.get("abstractZh");

		String	countryNameEn	=(String)map.get("countryNameEn");
		
		String	countryNameZh	=(String)map.get("countryNameZh");
		
		String	districtNameEn	=(String)map.get("districtNameEn");
		
		String	districtNameZh	=(String)map.get("districtNameZh");

		String	domain			=(String)map.get("domain");

		int	eventCountryId	= 0;      		//***int not long 
			v_int 		= (Integer)map.get("eventCountryId");
		if( v_int != null)
				eventCountryId  = v_int.intValue();
		
		String keywordsEn="";
		try{
			v_ja		=(JSONArray)map.get("keywordsEn");
			keywordsEn = v_ja.toString();
		}catch(Exception e){
			keywordsEn =(String)map.get("keywordsEn");
		}
		
		String keywordsZh="";
		try{
			v_ja		=(JSONArray)map.get("keywordsZh");
			keywordsZh = v_ja.toString();
		}catch(Exception e){
			keywordsZh =(String)map.get("keywordsZh");
		}
		
		String	languageTname	=(String)map.get("languageTname");
		
		String	latitude		="";
		try{
			latitude		=(String)map.get("latitude");
		}catch(Exception e){
			v_ja =(JSONArray)map.get("latitude");
			latitude = v_ja.toString();
		}

		int		locationId		= 0;
			v_int 		= (Integer)map.get("locationId");
		if( v_int != null)
				 locationId = v_int.intValue();
		
		String	longitude		="";
		try{
			longitude		=(String)map.get("longitude");
		}catch(Exception e){
			v_ja =(JSONArray)map.get("longitude");
			longitude = v_ja.toString();
		}

		int		mediaLevel		= 0;
			v_int 		= (Integer)map.get("mediaLevel");
		if( v_int != null)
				 mediaLevel = v_int.intValue();

		String	mediaNameSrc	=(String)map.get("mediaNameSrc");
		
		String	mediaTname		=(String)map.get("mediaTname");

		String	provinceNameEn	=(String)map.get("provinceNameEn");
		
		String	provinceNameZh	=(String)map.get("provinceNameZh");

		String	pubdate			=(String)map.get("pubdate");
		SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
			if(null!=pubdate&&""!=pubdate){
				Date date=dateFormat.parse(pubdate);
				pubdate=format.format(date);
			}

		int		sensitiveType	= 0;
			v_int 		= (Integer)map.get("sensitiveType");
		if( v_int != null)
				 sensitiveType = v_int.intValue();

		int		sentimentId		= 0;
			v_int 		= (Integer)map.get("sentimentId");
		if( v_int != null)
				 sentimentId = v_int.intValue();

		String tagId=(String)map.get("tagId");
		
		String	titleSrc		=(String)map.get("titleSrc");
		
		int		transfer		= 0;
			v_int 		= (Integer)map.get("transfer");
		if( v_int != null)
				 transfer = v_int.intValue();

		String	url				=(String)map.get("url");

		String	uuid			=(String)map.get("uuid");

		int		view			= 0;
			v_int 		= (Integer)map.get("view");
		if( v_int != null)
				 view = v_int.intValue();

		String	textSrc			=(String)map.get("textSrc");

		data=specialId			+Resource.BETWEEN
				+abstractZh	  		+Resource.BETWEEN
				+countryNameEn		+Resource.BETWEEN
				+countryNameZh		+Resource.BETWEEN
				+districtNameEn 	+Resource.BETWEEN
				+districtNameZh 	+Resource.BETWEEN
				+domain		  		+Resource.BETWEEN
				+eventCountryId   	+Resource.BETWEEN    
				+keywordsEn	  		+Resource.BETWEEN
				+keywordsZh	  		+Resource.BETWEEN
				+languageTname		+Resource.BETWEEN
				+latitude	  		+Resource.BETWEEN
				+locationId	      	+Resource.BETWEEN
				+longitude			+Resource.BETWEEN
				+mediaLevel	      	+Resource.BETWEEN    
				+mediaNameSrc 		+Resource.BETWEEN
				+mediaTname	  		+Resource.BETWEEN
				+provinceNameEn 	+Resource.BETWEEN
				+provinceNameZh 	+Resource.BETWEEN
				+pubdate	  		+Resource.BETWEEN
				+sensitiveType	  	+Resource.BETWEEN    
				+sentimentId	  	+Resource.BETWEEN
				+tagId				+Resource.BETWEEN
				+textSrc	  		+Resource.BETWEEN
				+titleSrc	  		+Resource.BETWEEN
				+transfer		  	+Resource.BETWEEN    
				+url		  		+Resource.BETWEEN
				+uuid		  		+Resource.BETWEEN
				+view;
		data=data.replace("\\", " ");
//		System.out.println(data);
		data=Resource.BEGIN+data;
		return data;
	}
	
	/*解析社交数据*/
	public String getSocData(Map<String, Object> map) throws Exception{
		String data="";
		Integer  v_int;
		Long v_long;
		String specialId=(String)map.get("specialId");
		
		int			atdCnt			= 0;
			v_int 		= (Integer)map.get("atdCnt");
		if( v_int != null)
				 atdCnt = v_int.intValue();

			String		city			=(String)map.get("city");
			
			int			cityId			= 0;
			v_int 		= (Integer)map.get("cityId");
		if( v_int != null)
				 cityId = v_int.intValue();

			int			cmtCnt			= 0;
			v_int 		= (Integer)map.get("cmtCnt");
		if( v_int != null)
				 cmtCnt = v_int.intValue();

			String		country			=(String)map.get("country");
			String country_EN="";
			String country_ZH="";
			if(null!=country&&""!=country){//将传过来的country转换并存入country_EN和country_ZH
				if(isEnglish(country)){
					country_EN=country;
					country_ZH=util.getCountry(country);
				}else{
					country_ZH=country;
					country_EN=util.getCountry(country);
				}
			}

			int			flwCnt			= 0;
			v_int 		= (Integer)map.get("flwCnt");
		if( v_int != null)
				 flwCnt = v_int.intValue();

			int			frdCnt			= 0;
			v_int 		= (Integer)map.get("frdCnt");
		if( v_int != null)
				 frdCnt = v_int.intValue();

			int			gender			= 0;
			v_int 		= (Integer)map.get("gender");
		if( v_int != null)
				 gender = v_int.intValue();


			String		id				=(String)map.get("id");

			int			isOri			= 0;
			v_int 		= (Integer)map.get("isOri");
		if( v_int != null)
				 isOri = v_int.intValue();


			String		languageCode	=(String)map.get("languageCode");

			String		myId			=(String)map.get("myId");
			
			String		name			=(String)map.get("name");
			
			String		province		=(String)map.get("province");

			int			provinceId		= 0;
			v_int 		= (Integer)map.get("provinceId");
		if( v_int != null)
				provinceId  = v_int.intValue();

			int			rpsCnt			= 0;
			v_int 		= (Integer)map.get("rpsCnt");
		if( v_int != null)
				 rpsCnt = v_int.intValue();

		int			sentimentOrient	= 0;
			v_int= (Integer)map.get("sentimentOrient");
			if(v_int != null)
				sentimentOrient = v_int.intValue();

			String		sourceType		=(String)map.get("sourceType");

			int			staCnt			= 0;
			v_int= (Integer)map.get("staCnt");
			if(v_int != null)
				staCnt = v_int.intValue();

			String		text			=(String)map.get("text");

			int			textLen			= 0;
			v_int= (Integer)map.get("textLen");
			if(v_int != null)
				textLen = v_int.intValue();

			long		time			= 0;
			try{
				v_long= (Long)map.get("time");
	 			if(v_int != null)
	 				time  = v_long.longValue();
			}catch(Exception e){
				Integer i=(Integer)map.get("time");
				time=i.longValue();
			}

			String		timeStr			=(String)map.get("timeStr");//将日期格式转换
			SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
			if(null!=timeStr&&""!=timeStr){
				Date date=dateFormat.parse(timeStr);
				timeStr=format.format(date);
			}
			
			String		title			=(String)map.get("title");

			String		url				=(String)map.get("url");
			
			String		userId			=(String)map.get("userId");
			
			String		userType		=(String)map.get("userType");

			int			verified		= 0;
			v_int= (Integer)map.get("verified");
			if(v_int != null)
				verified = v_int.intValue();
			
			long		view			= 0;
			v_int= (Integer)map.get("view");
			if(v_int != null)
				view = v_int.intValue();

		data=specialId				+Resource.BETWEEN
				+atdCnt					+Resource.BETWEEN
				+city					+Resource.BETWEEN
				+cityId					+Resource.BETWEEN   	
				+cmtCnt					+Resource.BETWEEN    
				+country_EN				+Resource.BETWEEN
				+country_ZH				+Resource.BETWEEN
				+flwCnt					+Resource.BETWEEN    
				+frdCnt					+Resource.BETWEEN   	
				+gender					+Resource.BETWEEN   	
				+id						+Resource.BETWEEN
				+isOri					+Resource.BETWEEN   	
				+languageCode			+Resource.BETWEEN
				+myId					+Resource.BETWEEN
				+name					+Resource.BETWEEN
				+province				+Resource.BETWEEN
				+provinceId				+Resource.BETWEEN    
				+rpsCnt					+Resource.BETWEEN    
				+sentimentOrient		+Resource.BETWEEN    
				+sourceType				+Resource.BETWEEN
				+staCnt					+Resource.BETWEEN    
				+text					+Resource.BETWEEN
				+textLen				+Resource.BETWEEN  	
				+time					+Resource.BETWEEN    
				+timeStr				+Resource.BETWEEN
				+title					+Resource.BETWEEN
				+url					+Resource.BETWEEN
				+userId					+Resource.BETWEEN
				+userType				+Resource.BETWEEN
				+verified				+Resource.BETWEEN    
				+view;
		data=data.replace("\\", " ");
//		System.out.println(data);
		data=Resource.BEGIN+data;
		return data;
	}
	
	public boolean isEnglish(String charaString){//判断country是否是中文
        return charaString.matches("^[a-zA-Z ]*");

    }
	
	public void doNews(String json) throws Exception {
		Map<String,Object> map=parseMsg(json);//将分隔符过滤后，将json解析成map对象
		if(null!=map&&map.size()>0){
			String specialId=(String)map.get("specialId");
			String fileDir="";
			if(null!=specialId&&!"".equals(specialId)){
				fileDir=getFileDir(specialId,"news");//拼接文件名
			}
			if(null!=fileDir&&!"".equals(fileDir)){
				Map<String, Object> historyMap=hdfsMap.get("history");
				if(!historyMap.containsKey(fileDir)){//检查是否是过期文件
					Map<String, Object> newsMap=hdfsMap.get("news");
					String state=(String)map.get("state");
					if(null==state){
						if(!newsMap.containsKey(fileDir)){//接到任务的第一条数据
							firstNews(map,newsMap,fileDir,specialId);
						}else{
							followingNews(map,newsMap,fileDir);
						}
					}else if(state.equals("EOF")){//接到结束标识
						newsDone(newsMap,fileDir,specialId);
					}else{//接到error标识
						localLog("news",specialId,3);
						HDFSLog(fileDir,"news",specialId,3);
					}
				}else{//如果是过期文件则不处理
					System.out.println("news  "+specialId+"  had finished , If you want to push again , please wait "+Resource.HISTRYTIME+" minutes");
				}
			}
		}
	}


	public void doSoc(String json)  throws Exception{
		Map<String,Object> map=parseMsg(json);//将分隔符过滤后，将json解析成map对象
		if(null!=map&&map.size()>0){
			String specialId=(String)map.get("specialId");
			String fileDir="";
			if(null!=specialId&&!"".equals(specialId)){
				fileDir=getFileDir(specialId,"soc");//将specialId解析并拼出文件存放目录
			}
			if(null!=fileDir&&!"".equals(fileDir)){
				Map<String, Object> historyMap=hdfsMap.get("history");
				if(!historyMap.containsKey(fileDir)){//判断是否是过期文件
					Map<String, Object> socMap=hdfsMap.get("soc");
					String state=(String)map.get("state");
					if(null==state){
						if(!socMap.containsKey(fileDir)){//接到任务的第一条数据
							firstSoc(map,socMap,fileDir,specialId);
						}else{
							followingSoc(map,socMap,fileDir);
						}
					}else if(state.equals("EOF")){//接到结束符
						socDone(socMap,fileDir,specialId);
					}else{//接到error
						localLog("soc",specialId,3);
						HDFSLog(fileDir,"soc",specialId,3);
					}
				}else{
					System.out.println("soc  "+specialId+"  had finished , If you want to push again , please wait "+Resource.HISTRYTIME+" minutes");
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private Map<String,Object> parseMsg(String json) {//解析json并替换关键字符
		Map<String,Object> maps = new Hashtable<String,Object>();
		if(null!=json&&""!=json){//过滤分隔符
			json=json.replace("\\n", " ");
			json=json.replace(Resource.BEGIN," ");
			json=json.replace(Resource.BETWEEN, ",");
			maps = (Map<String,Object>)JSON.parse(json);
		}
		return maps;
	}
	
	public String getFileDir(String specialId,String mt){
		String taskId="";
		String userId="";
		String[] ids=specialId.split("-");
		if(null!=ids){
			taskId=ids[0];//任务id
			userId=ids[1];//用户id
		}
		return Resource.SOURCEDIR+"/"+mt+"/"+userId+"/"+taskId;
	}

	/*第一条新闻数据*/
	public void firstNews(Map<String,Object> map,Map<String,Object> newsMap,String fileDir, String specialId){
		Map<String, Object> dataMap=new Hashtable<String, Object>();
		ArrayList<Map<String, Object>> list=new ArrayList<Map<String, Object>>();
		list.add(map);
		dataMap.put("date", new Date());
		dataMap.put("list", list);
		dataMap.put("specialId", specialId);
		newsMap.put(fileDir, dataMap);
		TimeOut timeOut=new TimeOut("news",fileDir,specialId);//判断是否超时
		timeOut.start();
	}
	
	/*第一条社交消息*/
	public void firstSoc(Map<String,Object> map,Map<String,Object> socMap,String fileDir, String specialId) throws Exception {
		Map<String, Object> dataMap=new Hashtable<String, Object>();
		dataMap.put("date", new Date());
		dataMap.put("specialId", specialId);
		String data=getSocData(map);
		
		if(null!=data&&!"".equals(data)){
			write(data,fileDir,dataMap);
		}
		socMap.put(fileDir, dataMap);
		TimeOut timeOut=new TimeOut("soc",fileDir,specialId);//判断是否超时
		timeOut.start();
	}
	
	/*第一条以外的新闻消息*/
	@SuppressWarnings("unchecked")
	private void followingNews(Map<String, Object> map, Map<String, Object> newsMap, String fileDir) throws Exception{
		Map<String, Object> dataMap=(Map<String, Object>)newsMap.get(fileDir);
		ArrayList<Map<String, Object>> list=(ArrayList<Map<String, Object>>)dataMap.get("list");
		if(null!=list){
			list.add(map);
			if(list.size()<Resource.IDLENGTH){
				dataMap.put("date", new Date());
			}else{
				dataMap.put("date", new Date());
				getText(list);
				writeNews(list,fileDir,dataMap);
				list.clear();
			}
		}
	}
	
	/*第一条以外的社交消息*/
	@SuppressWarnings("unchecked")
	public void followingSoc(Map<String, Object> map, Map<String, Object> socMap, String fileDir) throws Exception{
		Map<String, Object> dataMap=(Map<String, Object>)socMap.get(fileDir);
		dataMap.put("date", new Date());
		String data=getSocData(map);
		write(data,fileDir,dataMap);
	}
	
	@SuppressWarnings("unchecked")
	public void getText(ArrayList<Map<String, Object>> list) throws Exception{
		Map<String,List<String>> jsonMap=new Hashtable<String,List<String>>();
		List<String> uuidList=new ArrayList<String>();
		for(Map<String, Object> map:list){
			String uuid="";
			if(null!=(String)map.get("uuid")){
				uuid=(String)map.get("uuid");
			}
			uuidList.add(uuid);
		}
		jsonMap.put("nid", uuidList);//将参数装入map中
		String requestJson=JSON.toJSONString(jsonMap);//将参数转成json格式
		String resultJson=doRequest(requestJson);//请求Hbase接口
		Map<String,Object> resultMap=parseMsg(resultJson);//将结果过滤并解析
		if(null!=resultMap&&resultMap.size()>0){
			Map<String,String> textMap=(Map<String,String>)resultMap.get("returndata");
			for(Map<String, Object> map:list){//将结果依次装回list中
				String textSrc="";
				if(null!=(String)map.get("uuid")){
					String uuid=(String)map.get("uuid");
					textSrc=textMap.get(uuid);
					map.put("textSrc", textSrc);
				}
			}
		}
	}
	
	/* 请求Hbase接口得到正文数据 */
	public String doRequest(String requestJson) throws Exception {
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(Resource.INTERFACE);
		post.setHeader("Content-Type", "application/json");
		post.addHeader("Authorization", "Basic YWRtaW46");
		StringEntity entity = new StringEntity(requestJson, "utf-8");
		post.setEntity(entity);
		HttpResponse httpResponse = client.execute(post);
		InputStream inStream = httpResponse.getEntity().getContent();
		BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, "utf-8"));
		StringBuilder strber = new StringBuilder();
		String line = null;
		while ((line = reader.readLine()) != null){
			strber.append(line + "\n");
		}
		inStream.close();
		if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
//			System.out.println("请求服务器成功，做相应处理");
		} else {
			System.out.println("请求服务端失败");
		}
		return strber.toString();
	}
	
	/*新闻数据写入处理*/
	public void writeNews(ArrayList<Map<String, Object>> list, String fileDir, Map<String, Object> dataMap) throws Exception{
		for(Map<String, Object> map:list){
			String data=getNewsData(map);//将map中的数据拼成对应格式的字符串
			write(data,fileDir,dataMap);
		}
	}
	
	/*结束新闻写入过程*/
	@SuppressWarnings("unchecked")
	public void newsDone(Map<String, Object> newsMap, String fileDir, String specialId) throws Exception{
		if(newsMap.containsKey(fileDir)){//正常结束，将未写完的数据写完，关闭输出流
			Map<String, Object> dataMap=(Map<String, Object>)newsMap.get(fileDir);
			ArrayList<Map<String, Object>> list=(ArrayList<Map<String, Object>>)dataMap.get("list");
			if(null!=list&&list.size()>0){
				writeNews(list,fileDir,dataMap);
			}
			writeDone(dataMap);
			localLog("news",specialId,0);//在本地记录日志
			HDFSLog(fileDir,"news",specialId,0);//在Hdfs上记录日志
			newsMap.remove(fileDir);//删除newsMap中的句柄
			Map<String, Object> historyMap=hdfsMap.get("history");
			synchronized (historyMap) {
				historyMap.put(fileDir, new Date());//将文件记录到过期文件的map中
			}
		}else{//本次任务没有数据只有结束符，直接记录日志
			localLog("news",specialId,2);
			HDFSLog(fileDir,"news",specialId,2);
		}
	}
	
	/*结束社交写入过程*/
	@SuppressWarnings("unchecked")
	public void socDone(Map<String, Object> socMap, String fileDir, String specialId) throws Exception{
		if(socMap.containsKey(fileDir)){//正常结束，将未写完的数据写完，关闭输出流
			Map<String, Object> dataMap=(Map<String, Object>)socMap.get(fileDir);
			writeDone(dataMap);//关闭输出流
			localLog("soc",specialId,0);
			HDFSLog(fileDir,"soc",specialId,0);
			socMap.remove(fileDir);
			Map<String, Object> historyMap=hdfsMap.get("history");
			synchronized (historyMap) {
				historyMap.put(fileDir, new Date());
			}
		}else{//本次任务没有数据只有结束符，直接记录日志
			localLog("soc",specialId,2);
			HDFSLog(fileDir,"soc",specialId,2);
		}
	}

	/*向hdfs中写入数据*/
	public void write(String data, String fileDir, Map<String, Object> dataMap) throws Exception{
		if(!dataMap.containsKey("outPutStream")){//创建第一个文件
			firstWrite(data,fileDir,dataMap);
		}else{
			if(null!=dataMap.get("size")&&(Integer)dataMap.get("size")>1024*1024*Resource.SIZE){
				changeSuffix(data,fileDir,dataMap);//如果超过则更换文件
			}else if(null!=dataMap.get("size")){
				continueWrite(data,fileDir,dataMap);//如果没超过，则继续写
			}
		}
	}
	
	/*任务的第一次读写*/
	public void firstWrite(String data, String fileDir, Map<String, Object> dataMap) throws Exception{
		FSDataOutputStream outPutStream=fileSystem.create(new Path(fileDir+"/001.dat"));//生成第一个文件
		data=data.substring(Resource.BEGIN.length());//去掉第一条记录开头的分隔符
		outPutStream.write(data.getBytes("UTF-8"));
		dataMap.put("outPutStream", outPutStream);
		dataMap.put("suffix", 1);//文件名后缀
		dataMap.put("size", data.getBytes("UTF-8").length);
		dataMap.put("names",fileDir+"/001.dat");//拼接文件名
		System.out.println("First Write  !!!");
	}
	
	/*更换文件继续写*/
	public void changeSuffix(String data, String fileDir, Map<String, Object> dataMap) throws Exception{
		FSDataOutputStream outPutStream=(FSDataOutputStream)dataMap.get("outPutStream");
		if(null!=outPutStream){
			outPutStream.flush();
			outPutStream.close();
		}
		int suffix=(Integer)dataMap.get("suffix");
		dataMap.put("suffix", ++suffix);//文件后缀自增
		String zero="";
		if(suffix<10){
			zero="0";
		}
		String newName=fileDir+"/0"+zero+suffix+".dat";
		FSDataOutputStream newStream=fileSystem.create(new Path(newName));
		data=data.substring(Resource.BEGIN.length());
		newStream.write(data.getBytes("UTF-8"));
		dataMap.put("size", data.getBytes("UTF-8").length);//重置文件大小
		dataMap.put("names", (String)dataMap.get("names")+"&&"+newName);//拼接新的文件名
		dataMap.put("outPutStream", newStream);
		System.out.println("Change Suffix  !!!");
	}
	
	/*不更换文件继续写*/
	public void continueWrite(String data, String fileDir, Map<String, Object> dataMap) throws Exception{
		FSDataOutputStream outPutStream=(FSDataOutputStream)dataMap.get("outPutStream");
		if(null!=outPutStream){
			outPutStream.write(data.getBytes("UTF-8"));
		}
		int size=(Integer)dataMap.get("size");
		size+=data.getBytes("UTF-8").length;
		dataMap.put("size",size);//更新文件大小
//		System.out.println("Continue Write  !!!");
	}
	
	/*完成读写过程*/
	public void writeDone(Map<String, Object> dataMap) throws Exception{//关闭输出流
		FSDataOutputStream outPutStream=(FSDataOutputStream)dataMap.get("outPutStream");
		if(null!=outPutStream){
			outPutStream.flush();
			outPutStream.close();
		}
	}
	
	public static void localLog(String mt, String specialId, int flag) throws Exception{
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		PrintStream log = new PrintStream("./logs/"+mt+"-"+specialId+".log");//记录到项目本地的日志文件名
		PrintStream out = System.out;
		System.setOut(log);
		String detail="";
		if(0==flag){
			detail="Success";
		}else if(1==flag){
			detail="TimeOut";
		}else if(2==flag){
			detail="NoneSource";
		}else if(3==flag){
			detail="TransferError";
		}
		String msg="Task  "+specialId+"  "+mt+"  "+detail+"  at "+format.format(new Date());
		System.out.println(msg);
		System.setOut(out);
		System.out.println("Local Log  !!!");
	}
	
	public static void HDFSLog(String fileDir, String mt, String specialId, int flag) throws Exception{
		String logName=Resource.LOGSDIR+"/"+mt+specialId+".log";//日志文件名
		FSDataOutputStream outPutStream=fileSystem.create(new Path(logName));
		String msg=flag+"|"+fileDir;//要写入文件的内容
		outPutStream.write(msg.getBytes("UTF-8"));
		outPutStream.flush();
		outPutStream.close();
		System.out.println("HDFS Log  !!!");
	}
}
