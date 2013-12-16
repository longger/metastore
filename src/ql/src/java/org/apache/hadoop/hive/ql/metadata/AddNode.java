package org.apache.hadoop.hive.ql.metadata;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.Explain;

public class AddNode implements Serializable {

  private static final long serialVersionUID = 1L;

  String nodeName;
  Integer status;
  List<String> ip;
  Map<String, String> nodeProperties;

  /**
   * For serialization only.
   */
  public AddNode() {
  }

  public AddNode(String nodeName, Integer status,
      List<String> ip) {
    super();
    this.nodeName = nodeName;
    this.status = status;
    this.ip = ip;
    this.nodeProperties = null;
  }

  public AddNode(String nodeName, Integer status, List<String> ip, Map<String, String> nodeProperties) {
    super();
    this.nodeName = nodeName;
    this.status = status;
    this.ip = ip;
    this.nodeProperties = nodeProperties;
  }

  public Map<String, String> getNodeProperties() {
    return nodeProperties;
  }

  public void setNodeProperties(Map<String, String> nodeProps) {
    this.nodeProperties = nodeProps;
  }

  @Explain(displayName="name")
  public String getName() {
    return nodeName;
  }

  public void setName(String nodeName) {
    this.nodeName = nodeName;
  }

  @Explain(displayName="status")
  public Integer getStatus() {
    return status;
  }

  public void setStatus(Integer status) {
    this.status = status;
  }

  @Explain(displayName="ip")
  public List<String> getIp() {
    return ip;
  }

  public void setIp(List<String> ip) {
    this.ip = ip;
  }

}
