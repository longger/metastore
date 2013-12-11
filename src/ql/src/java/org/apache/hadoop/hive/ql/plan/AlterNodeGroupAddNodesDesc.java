package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * AlterNodeGroupAddNodesDesc.
 *
 */
@Explain(displayName = "Alter NodeGroupDesc Add Nodes")
public class AlterNodeGroupAddNodesDesc extends DDLDesc implements Serializable {

    private static final long serialVersionUID = 1L;
    String nodeGroupName;
    String comment;
    Map<String, String> nodeGroupProps;
    Set<String> nodes;

    /**
     * For serialization only.
     */

    public AlterNodeGroupAddNodesDesc() {
      super();
    }

    public AlterNodeGroupAddNodesDesc(String nodeGroupName, String comment,
        Set<String> nodes) {
      super();
      this.nodeGroupName = nodeGroupName;
      this.comment = comment;
      this.nodes = nodes;
    }

    public AlterNodeGroupAddNodesDesc(String nodeGroupName) {
      this(nodeGroupName, null, null);
    }

    public Map<String, String> getNodeGroupProps() {
      return nodeGroupProps;
    }

    public void setNodeGroupProps(Map<String, String> nodeGroupProps) {
      this.nodeGroupProps = nodeGroupProps;
    }

    @Explain(displayName="name")
    public String getNodeGroupName() {
      return nodeGroupName;
    }

    public void setNodeGroupName(String nodeGroupName) {
      this.nodeGroupName = nodeGroupName;
    }

    @Explain(displayName="comment")
    public String getComment() {
      return comment;
    }

    public void setComment(String comment) {
      this.comment = comment;
    }

    @Explain(displayName="nodes")
    public Set<String> getNodes() {
      return nodes;
    }

    public void setNodes(Set<String> nodes) {
      this.nodes = nodes;
    }

}
