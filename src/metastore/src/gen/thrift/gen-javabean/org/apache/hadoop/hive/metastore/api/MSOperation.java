/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum MSOperation implements org.apache.thrift.TEnum {
  EXPLAIN(1),
  CREATEDATABASE(2),
  DROPDATABASE(3),
  DROPTABLE(4),
  DESCTABLE(5),
  ALTERTABLE_RENAME(6),
  ALTERTABLE_RENAMECOL(7),
  ALTERTABLE_ADDPARTS(8),
  ALTERTABLE_DROPPARTS(9),
  ALTERTABLE_ADDCOLS(10),
  ALTERTABLE_REPLACECOLS(11),
  ALTERTABLE_RENAMEPART(12),
  ALTERTABLE_PROPERTIES(13),
  SHOWDATABASES(14),
  SHOWTABLES(15),
  SHOWCOLUMNS(16),
  SHOW_TABLESTATUS(17),
  SHOW_TBLPROPERTIES(18),
  SHOW_CREATETABLE(19),
  SHOWINDEXES(20),
  SHOWPARTITIONS(21),
  CREATEVIEW(22),
  DROPVIEW(23),
  CREATEINDEX(24),
  DROPINDEX(25),
  ALTERINDEX_REBUILD(26),
  ALTERVIEW_PROPERTIES(27),
  CREATEUSER(28),
  DROPUSER(29),
  CHANGE_PWD(30),
  AUTHENTICATION(31),
  SHOW_USERNAMES(32),
  CREATEROLE(33),
  DROPROLE(34),
  GRANT_PRIVILEGE(35),
  REVOKE_PRIVILEGE(36),
  SHOW_GRANT(37),
  GRANT_ROLE(38),
  REVOKE_ROLE(39),
  SHOW_ROLE_GRANT(40),
  CREATETABLE(41),
  QUERY(42),
  ALTERINDEX_PROPS(43),
  ALTERDATABASE(44),
  DESCDATABASE(45);

  private final int value;

  private MSOperation(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static MSOperation findByValue(int value) { 
    switch (value) {
      case 1:
        return EXPLAIN;
      case 2:
        return CREATEDATABASE;
      case 3:
        return DROPDATABASE;
      case 4:
        return DROPTABLE;
      case 5:
        return DESCTABLE;
      case 6:
        return ALTERTABLE_RENAME;
      case 7:
        return ALTERTABLE_RENAMECOL;
      case 8:
        return ALTERTABLE_ADDPARTS;
      case 9:
        return ALTERTABLE_DROPPARTS;
      case 10:
        return ALTERTABLE_ADDCOLS;
      case 11:
        return ALTERTABLE_REPLACECOLS;
      case 12:
        return ALTERTABLE_RENAMEPART;
      case 13:
        return ALTERTABLE_PROPERTIES;
      case 14:
        return SHOWDATABASES;
      case 15:
        return SHOWTABLES;
      case 16:
        return SHOWCOLUMNS;
      case 17:
        return SHOW_TABLESTATUS;
      case 18:
        return SHOW_TBLPROPERTIES;
      case 19:
        return SHOW_CREATETABLE;
      case 20:
        return SHOWINDEXES;
      case 21:
        return SHOWPARTITIONS;
      case 22:
        return CREATEVIEW;
      case 23:
        return DROPVIEW;
      case 24:
        return CREATEINDEX;
      case 25:
        return DROPINDEX;
      case 26:
        return ALTERINDEX_REBUILD;
      case 27:
        return ALTERVIEW_PROPERTIES;
      case 28:
        return CREATEUSER;
      case 29:
        return DROPUSER;
      case 30:
        return CHANGE_PWD;
      case 31:
        return AUTHENTICATION;
      case 32:
        return SHOW_USERNAMES;
      case 33:
        return CREATEROLE;
      case 34:
        return DROPROLE;
      case 35:
        return GRANT_PRIVILEGE;
      case 36:
        return REVOKE_PRIVILEGE;
      case 37:
        return SHOW_GRANT;
      case 38:
        return GRANT_ROLE;
      case 39:
        return REVOKE_ROLE;
      case 40:
        return SHOW_ROLE_GRANT;
      case 41:
        return CREATETABLE;
      case 42:
        return QUERY;
      case 43:
        return ALTERINDEX_PROPS;
      case 44:
        return ALTERDATABASE;
      case 45:
        return DESCDATABASE;
      default:
        return null;
    }
  }
}
