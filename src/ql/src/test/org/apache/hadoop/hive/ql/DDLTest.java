package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;

public class DDLTest {

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

    //String sql = "create geoloc('aaa','qw','abc','sd','ff')";
    //String sql = "drop geoloc'aaa'";
    //String sql = "modify geoloc('aaa','qq','abc','sd','ff')";
    //String sql = "show geoloc";

    //String sql = "create eqroom('aaa',qw)comment 'dsds' on'ff'";
    //String sql = "drop eqroom'aaa'";
    //String sql = "modify eqroom('aaa',qq)comment 'dsds' on'ff'";
    //String sql = "show eqroom";

    //String sql = "create NODEASSIGNMENT('aaa','db1')";
    //String sql = "DROP NODEASSIGNMENT('aaa','qw')";
    //String sql = "show NODEASSIGNMENT";

    //String sql = "create nodeGroupAssignment('a','b')";
    //String sql = "DROP nodeGroupAssignment('a','b')";
    //String sql = "show nodeGroupAssignment";

    //String sql = "create userAssignment('a','b')";
    //String sql = "drop userAssignment('a','b')";
    //String sql = "show userAssignment";

    //String sql = "create roleAssignment('a','b')";
    //String sql = "drop roleAssignment('a','b')";
    //String sql = "show roleAssignment";

    //String sql = "alter schema tb rename to tc";
    //String sql = "alter schema tb add columns(col int)";
    //String sql = "alter schema tb change column tb tc int";
    //String sql = "alter schema tb set schemeproperties('ff'='FF')";

    //String sql = "drop schema z_q_h"
    //String sql = "grant all on database dbzqh to user root";
    //String sql = "alter table t_cdr add distribute on nodegroup 'XX'";
    //String sql = "alter nodegroup zz delete nodes 'NODE34'";

    String sql =  "alter table t_dx_rz_ccdx change c_ydz c_ydz string comment '@tel'";
    Driver dr = new Driver(new HiveConf());
    try {
      dr.run(sql);
    } catch (CommandNeedRetryException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    dr.compile(sql);
  }

}
