package com.bonc.kafkamsg;



/**
 * Unit test for simple App.
 */
public class AppTest 
{
    public static void main(String[] args) {
//    	Map<String,List<String>> map=new Hashtable<String,List<String>>();
//    	List<String> list=new ArrayList<String>();
//    	list.add("");
//    	list.add("a");
//    	list.add("a");
//    	list.add("a");
//    	list.add("a");
//    	list.add("a");
//    	list.add("a");
//    	map.put("nid", list);
//		String json=JSON.toJSONString(map);
//		System.out.println(json);
    	String s=Resource.BEGIN+"aaa\\aaaaa"+Resource.BEGIN+"jjjjj\\jjjj"+Resource.BEGIN+"jjjjj\\jjjj";
    	String n=s.replace("\\", " ");
    	System.out.println(n);
	}
}
