package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Set;

/**
 * AlterNodeGroupDeleteNodesDesc.
 *
 */
@Explain(displayName = "Alter NodeGroupDesc Delete Nodes")
public class AlterNodeGroupDeleteNodesDesc extends DDLDesc implements Serializable {

    private static final long serialVersionUID = 1L;
    String nodeGroupName;
    Set<String> nodes;

    /**
     * For serialization only.
     */

    public AlterNodeGroupDeleteNodesDesc() {
      super();
    }

    public AlterNodeGroupDeleteNodesDesc(String nodeGroupName,
        Set<String> nodes) {
      super();
      this.nodeGroupName = nodeGroupName;
      this.nodes = nodes;
    }

    @Explain(displayName="name")
    public String getNodeGroupName() {
      return nodeGroupName;
    }

    public void setNodeGroupName(String nodeGroupName) {
      this.nodeGroupName = nodeGroupName;
    }

    @Explain(displayName="nodes")
    public Set<String> getNodes() {
      return nodes;
    }

    public void setNodes(Set<String> nodes) {
      this.nodes = nodes;
    }

}
