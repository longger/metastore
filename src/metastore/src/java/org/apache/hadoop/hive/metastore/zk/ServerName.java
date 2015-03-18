package org.apache.hadoop.hive.metastore.zk;

import java.net.UnknownHostException;

//import com.sun.xml.internal.ws.encoding.soap.DeserializationException;

public class ServerName {

  String server;

  public ServerName(String name){
    this.server = name;
  }

  public static ServerName parseFrom(byte[] data)  {
    return new ServerName(new String(data));
  }

  public static byte[] toByteArray(ServerName sn) {
    // TODO Auto-generated method stub
    return sn.server.getBytes();
  }

  public static boolean isSameHostnameAndPort(ServerName currentMaster, ServerName sn) {

    return currentMaster.server.equalsIgnoreCase(sn.server);
  }

  public static String getLocalhost(){
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      return "";
    }
  }

  @Override
  public String toString(){
    return this.server;
  }

}
