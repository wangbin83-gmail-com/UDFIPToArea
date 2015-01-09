package com.jd.ipaddress;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFIpToArea
  extends UDF
{
  public static HashMap<String, String> contentMap = null;
  public static String[] ipArray = null;
  public static String IPLIB = "";
  public static String IPCONTENT = "";
  public static final String UNKNOWN = "未知区域";
  public static final Log LOG = LogFactory.getLog(UDFIpToArea.class);
  public static Properties props = new Properties();
  
  static {
	  InputStream in = UDFIpToArea.class.getResourceAsStream("/conf.properties");
	  try {
		props.load(in);
	  } catch (IOException e) {
		System.exit(-1);
	  }
	  IPLIB = props.getProperty("IPLIB");
	  IPCONTENT =  props.getProperty("IPCONTENT");
  }
  
  public String evaluate(String str, String arg)
  {
    try
    {
      if ((!"all".equalsIgnoreCase(arg)) && (!"country".equalsIgnoreCase(arg)) && 
        (!"province".equalsIgnoreCase(arg)) && 
        (!"city".equalsIgnoreCase(arg)) && (!"area".equalsIgnoreCase(arg)) && (!"type".equalsIgnoreCase(arg)) && 
        (!"code".equalsIgnoreCase(arg))) {
        return "未知区域";
      }
      String argString = arg;
      long ip = analysisIP(str);
      if (ip == 0L) {
        return "未知区域";
      }
      loadLib();
      return getIPArea(ip, argString);
    }
    catch (Exception e) {}
    return "未知区域";
  }
  
  public String evaluate(String str)
  {
    try
    {
      String argString = "all";
      if ((!"all".equalsIgnoreCase(argString)) && (!"country".equalsIgnoreCase(argString)) && 
        (!"province".equalsIgnoreCase(argString)) && 
        (!"city".equalsIgnoreCase(argString)) && (!"area".equalsIgnoreCase(argString)) && (!"type".equalsIgnoreCase(argString))) {
        return "未知区域";
      }
      long ip = analysisIP(str);
      if (ip == 0L) {
        return "未知区域";
      }
      loadLib();
      return getIPArea(ip, argString);
    }
    catch (Exception e) {}
    return "未知区域";
  }
  
  public void loadLib()
  {
    try
    {
      if (ipArray == null) {
        loadIPLIB();
      }
      if (contentMap == null) {
        loadIpContent();
      }
    }
    catch (Exception localException) {}
  }
  
  public String getIPArea(long ip, String argString)
  {
    String returnResult = "未知区域";
    String middleStr = null;
    long startIP = 0L;
    long endIP = 0L;
    int code = 0;
    String[] strs = new String[10];
    
    int front = 0;
    int tail = ipArray.length - 1;
    while (front <= tail)
    {
      int middle = (front + tail) / 2;
      middleStr = ipArray[middle];
      strs = middleStr.split(" ");
      if (strs.length >= 2)
      {
        startIP = Long.parseLong(strs[0]);
        endIP = Long.parseLong(strs[1]);
        code = Integer.valueOf(strs[2]).intValue();
      }
      if ((startIP == ip) || (endIP == ip))
      {
        returnResult = new String(middleStr.substring(middleStr.indexOf(" ", middleStr.indexOf(" ") + 1) + 1, middleStr.length()).toCharArray());
        break;
      }
      if ((ip >= startIP) && (ip <= endIP))
      {
        returnResult = new String(middleStr.substring(middleStr.indexOf(" ", middleStr.indexOf(" ") + 1) + 1, middleStr.length()).toCharArray());
        break;
      }
      if (startIP > ip) {
        tail = middle - 1;
      } else if (endIP < ip) {
        front = middle + 1;
      }
    }
    if ((!"未知区域".equals(returnResult)) && (returnResult != null) && (contentMap != null))
    {
      returnResult = new String((String)contentMap.get(returnResult));
      if ("all".equalsIgnoreCase(argString)) {
        returnResult = returnResult.replaceAll(",", " ").trim().replaceAll(" +", " ");
      } else if ("country".equalsIgnoreCase(argString)) {
        returnResult = returnResult.split(",")[0];
      } else if ("province".equalsIgnoreCase(argString)) {
        returnResult = returnResult.split(",")[1];
      } else if ("city".equalsIgnoreCase(argString))
      {
        if ("中国".equals(returnResult.split(",")[0])) {
          returnResult = returnResult.split(",")[2];
        } else {
          returnResult = returnResult.split(",")[6];
        }
      }
      else if ("area".equalsIgnoreCase(argString)) {
        returnResult = returnResult.split(",")[3];
      } else if ("type".equalsIgnoreCase(argString)) {
        returnResult = returnResult.split(",")[4];
      } else if ("code".equalsIgnoreCase(argString)) {
        returnResult = String.valueOf(code);
      }
    }
    return returnResult;
  }
  
  public long analysisIP(String ip)
  {
    long returnIp = 0L;
    if (ip.split("\\.").length == 4) {
      try
      {
        String[] ipStrArray = ip.split("\\.");
        long[] ipLongArray = new long[4];
        for (int i = 0; i < ipStrArray.length; i++) {
          ipLongArray[i] = Long.parseLong(ipStrArray[i]);
        }
        returnIp = (ipLongArray[0] << 24) + (ipLongArray[1] << 16) + (ipLongArray[2] << 8) + ipLongArray[3];
      }
      catch (Exception localException) {}
    }
    return returnIp;
  }
  
  public void loadIPLIB()
    throws Exception
  {
    List<String> ipArrayList = new ArrayList(3900000);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(IPLIB), conf);
    FSDataInputStream hdfsInStream = fs.open(new Path(IPLIB));
    BufferedReader bufferedReader = 
      new BufferedReader(new InputStreamReader(hdfsInStream));
    String tempString = null;
    while ((tempString = bufferedReader.readLine()) != null) {
      ipArrayList.add(tempString);
    }
    bufferedReader.close();
    hdfsInStream.close();
//    fs.close();
    ipArray = (String[])ipArrayList.toArray(new String[ipArrayList.size()]);
    ipArrayList.clear();
    LOG.info("loaded " + ipArray.length + " ips");
  }
  
  public void loadIpContent()
    throws IOException
  {
    contentMap = new HashMap(130000);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(IPCONTENT), conf);
    FSDataInputStream hdfsInStream = fs.open(new Path(IPCONTENT));
    BufferedReader bufferedReader = 
      new BufferedReader(new InputStreamReader(hdfsInStream, "utf-8"));
    String contentString = null;
    while ((contentString = bufferedReader.readLine()) != null)
    {
      String[] strs = contentString.split("\\|");
      if (strs.length >= 2) {
        contentMap.put(strs[0], strs[1]);
      }
    }
    bufferedReader.close();
    hdfsInStream.close();
    LOG.info("loaded " + contentMap.size() + " content");
//    fs.close();
  }
}

