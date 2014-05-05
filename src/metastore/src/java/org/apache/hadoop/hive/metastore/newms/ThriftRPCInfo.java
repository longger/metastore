package org.apache.hadoop.hive.metastore.newms;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class ThriftRPCInfo {
  private static final AtomicLong TOTAL_COUNT = new AtomicLong();
  private static final Date CURRENT_DATE = new Date();
  private static final String FORMAT = "Report_For_Date:\t%s%n" +
      "RPC_Count:\t%s%n" +
      "RPC_Average_Time:\t%.2f%n" +
      "RPC_Max_Method:\t%s%n" +
      "RPC_Max_Time:\t%.2f%n" +
      "RPC_Min_Method:\t%s%n" +
      "RPC_Min_Time:\t%.2f%n";

  private static final ConcurrentHashMap<String, _Info> info = new ConcurrentHashMap<String, ThriftRPCInfo._Info>();
  private static final AtomicLong totalTime = new AtomicLong();

  private final Comparator<Entry<String, _Info>> comp = new Comparator<Entry<String, _Info>>() {
    @Override
    public int compare(Entry<String, _Info> o1, Entry<String, _Info> o2) {
      return (o2.getValue().avg() - o1.getValue().avg()) > 0 ? 1 : -1;
    }
  };
  // 初始化时为ThriftRPC类中所有的公有方法初始一个_Info
  static {
    Class<ThriftRPC> clazz = ThriftRPC.class;
    Method[] methods = clazz.getMethods();
    //重载的多个方法有相同的方法名
    for (Method m : methods) {
//      if (m.isAccessible()) {
        info.put(m.getName(), new _Info());
//      }
    }
  }

  public void captureInfo(String methodName, long time) {
    TOTAL_COUNT.incrementAndGet();
    info.get(methodName).addTime(time);
    totalTime.addAndGet(time);
  }

  public void dumpToFile(String path) throws IOException {
    if (Strings.isNullOrEmpty(path)) {
      throw new IOException("Invalid path");
    }
    File file = new File(path);
    if (!file.isFile()) {
      throw new IOException(String.format("'%s' is not a file", path));
    }
    List<Entry<String,_Info>> entries = Lists.newArrayList(info.entrySet());
    Collections.sort(entries, comp);
    StringBuilder sb = new StringBuilder();
    sb.append(new Date());
    //sb.append(String.format("Name\t\tMax\t\tMin\t\tAvg\t\t\n"));
    for (Entry<String, _Info> e : entries) {
      sb.append(String.format("%-8s\t%-4.2f\t%-4.2f\t%-8.2f\t\n", e.getKey(),
          e.getValue().getMax(), e.getValue().getMin(), e.getValue().avg()));
    }
    // write sb to filep
    FileOutputStream fos = new FileOutputStream(file, true);
    fos.write(sb.toString().getBytes());
    fos.close();
//    Files.write(sb.toString().getBytes(), file);
  }

  @Override
  public String toString() {
    List<Entry<String,_Info>> entries = Lists.newArrayList(info.entrySet());
    Collections.sort(entries, comp);
    // 统计信息排除没有被调用过的方法
    int last;
    for (last = entries.size()- 1; last >= 0; last--) {
      if (entries.get(last).getValue().getCount() > 0) {
        break;
      }
    }
    if(last < 0) {
      return "no rpc called";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(FORMAT,
        CURRENT_DATE, TOTAL_COUNT.get(),
        (double) totalTime.get() / TOTAL_COUNT.get() / 1000,
        entries.get(0).getKey(),// RPC_Max_Method
        entries.get(0).getValue().avg() / 1000,// RPC_Max_Time
        entries.get(last).getKey(),// RPC_Min_Method
        entries.get(last).getValue().avg() / 1000// RPC_Min_Time
        )).append("RPC_TOP_10:\t\n");

    for (int i = 0; i < 10; i++) {
      sb.append(String.format("%s:\tavg=%.2f\tmax=%s\tmin=%s\n",
          entries.get(i).getKey(),
          entries.get(i).getValue().avg() / 1000.0,
          (entries.get(i).getValue().getMax() == Long.MIN_VALUE ? "MIN" : String.format("%.2f", (entries.get(i).getValue().getMax() / 1000.0))),
          (entries.get(i).getValue().getMin() == Long.MAX_VALUE ? "MAX" : String.format("%.2f", (entries.get(i).getValue().getMin() / 1000.0)))));
    }
    sb.append("\n");
    return sb.toString();
  }

  private static class _Info implements Comparable<_Info> {
    private final AtomicLong count = new AtomicLong();
    private Double avg = new Double(0.0);
    private Long max = Long.MIN_VALUE;
    private Long min = Long.MAX_VALUE;

    public Double avg() {
      return avg;
    }

    public void addTime(Long time) {
      avg = ((count.get()) * avg + time) / count.incrementAndGet();
      if (time > max) {
        max = time;
      } else if (time < min) {
        min = time;
      }
    }

    public Long getCount() {
      return count.get();
    }

    public Long getMax() {
      return this.max;
    }

    public Long getMin() {
      return this.min;
    }

    @Override
    public int compareTo(_Info o) {
      return (int) (this.avg() - o.avg());
    }
  }
}
