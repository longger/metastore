package org.apache.hadoop.hive.metastore.newms;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ThriftRPCInfo {
  private static final AtomicLong TOTAL_COUNT = new AtomicLong();
  private static final Date CURRENT_DATE = new Date();
  private static final String FORMAT = "Report_Form_Date:\t%s%n" +
      "RPC_Count:\t%s%n" +
      "RPC_Average_Time:\t%.2f%n" +
      "RPC_Max_Time:\t%s%n" +
      "RPC_Max_Method:\t%s%n" +
      "RPC_Min_Time:\t%s%n" +
      "RPC_Min_Method:\t%s%n";
  private static final ConcurrentHashMap<String, _Info> info = new ConcurrentHashMap<String, ThriftRPCInfo._Info>();
  private final AtomicLong totalTime = new AtomicLong();

  static {// 初始化时为ThriftRPC类中所有的公有方法初始一个_Info
    Class<ThriftRPC> clazz = ThriftRPC.class;
    Method[] methods = clazz.getMethods();
    for (Method m : methods) {
      if (m.isAccessible()) {
        info.put(m.getName(), new _Info());
      }
    }
  }

  public void captureInfo(String methodName, long time) {
    TOTAL_COUNT.incrementAndGet();
    info.get(methodName).addTime(time);
    totalTime.addAndGet(time);
  }

  @Override
  public String toString() {
    Entry<String, _Info>[] entries = (Entry<String, _Info>[]) info.entrySet().toArray();
    Arrays.sort(entries, new Comparator<Entry<String, _Info>>() {
      @Override
      public int compare(Entry<String, _Info> o1, Entry<String, _Info> o2) {
        return (int) (o1.getValue().avg() - o2.getValue().avg());
      }

    });
    // 统计信息排除没有被调用过的方法
    int last;
    for (last = entries.length - 1; last >= 0; last--) {
      if (entries[last].getValue().getCount() > 0) {
        break;
      }
    }
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(FORMAT,
        CURRENT_DATE, TOTAL_COUNT.get(),
        totalTime.get() / TOTAL_COUNT.get(),
        entries[0].getValue().avg(),// RPC_Max_Time
        entries[0].getKey(),// RPC_Max_Method
        entries[last].getValue(),// RPC_Min_Time
        entries[last].getKey()
        )).append("RPC_TOP_10:\t");
    for (int i = 0; i < 10; i++) {
      sb.append(entries[i].getKey()).append(":").append(entries[i].getValue().avg()).append("\t");
    }
    sb.append("\n");
    return sb.toString();
  }

  private static class _Info implements Comparable<_Info> {
    private final AtomicLong count = new AtomicLong();
    private Double avg;
    private Long max = Long.MAX_VALUE;
    private Long min = Long.MIN_VALUE;

    public Double avg() {
      return avg;
    }

    public void addTime(Long time) {
      avg = ((count.get()) * avg + time) / count.incrementAndGet();
      if(time>max) {
        max=time;
      } else if(time<min) {
        min = time;
      }
    }

    public Long getCount() {
      return count.get();
    }
    public Long getMax(){
      return this.max;
    }
    public Long getMin(){
      return this.min;
    }
    @Override
    public int compareTo(_Info o) {
      return (int) (this.avg() - o.avg());
    }
  }
}
