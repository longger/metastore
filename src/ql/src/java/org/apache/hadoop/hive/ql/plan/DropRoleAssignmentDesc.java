package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

/**
 * DropRoleAssignmentDesc.
 *
 */
@Explain(displayName = "Drop RoleAssignment")
public class DropRoleAssignmentDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  String dbName;
  String roleName;

  public DropRoleAssignmentDesc() {
  }

  public DropRoleAssignmentDesc(String dbName, String roleName) {
    super();
    this.dbName = dbName;
    this.roleName = roleName;
  }


  @Explain(displayName="RoleName")
  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  @Explain(displayName="DbName")
  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }


}
