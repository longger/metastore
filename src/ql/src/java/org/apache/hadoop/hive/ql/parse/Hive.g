/**
   LialterStatementChangeColPositionoundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
grammar Hive;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}

tokens {
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_SELECTDI;
TOK_SELEXPR;
TOK_FROM;
TOK_TAB;
TOK_PARTSPEC;
TOK_PARTVAL;
TOK_DIR;
TOK_LOCAL_DIR;
TOK_TABREF;
TOK_SUBQUERY;
TOK_INSERT_INTO;
TOK_DESTINATION;
TOK_ALLCOLREF;
TOK_TABLE_OR_COL;
TOK_FUNCTION;
TOK_FUNCTIONDI;
TOK_FUNCTIONSTAR;
TOK_WHERE;
TOK_OP_EQ;
TOK_OP_NE;
TOK_OP_LE;
TOK_OP_LT;
TOK_OP_GE;
TOK_OP_GT;
TOK_OP_DIV;
TOK_OP_ADD;
TOK_OP_SUB;
TOK_OP_MUL;
TOK_OP_MOD;
TOK_OP_BITAND;
TOK_OP_BITNOT;
TOK_OP_BITOR;
TOK_OP_BITXOR;
TOK_OP_AND;
TOK_OP_OR;
TOK_OP_NOT;
TOK_OP_LIKE;
TOK_TRUE;
TOK_FALSE;
TOK_TRANSFORM;
TOK_SERDE;
TOK_SERDENAME;
TOK_SERDEPROPS;
TOK_EXPLIST;
TOK_ALIASLIST;
TOK_GROUPBY;
TOK_ROLLUP_GROUPBY;
TOK_CUBE_GROUPBY;
TOK_GROUPING_SETS;
TOK_GROUPING_SETS_EXPRESSION;
TOK_HAVING;
TOK_ORDERBY;
TOK_CLUSTERBY;
TOK_DISTRIBUTEBY;
TOK_SORTBY;
TOK_UNION;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_UNIQUEJOIN;
TOK_CROSSJOIN;
TOK_LOAD;
TOK_EXPORT;
TOK_IMPORT;
TOK_NULL;
TOK_ISNULL;
TOK_ISNOTNULL;
TOK_TINYINT;
TOK_SMALLINT;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FLOAT;
TOK_DOUBLE;
TOK_DATE;
TOK_DATETIME;
TOK_TIMESTAMP;
TOK_STRING;
TOK_BINARY;
TOK_BLOB;
TOK_LIST;
TOK_STRUCT;
TOK_MAP;
TOK_UNIONTYPE;
TOK_COLTYPELIST;
TOK_CREATEDATABASE;
TOK_CREATETABLE;
TOK_CREATEINDEX;
TOK_CREATEINDEX_INDEXTBLNAME;
TOK_DEFERRED_REBUILDINDEX;
TOK_DROPINDEX;
TOK_LIKETABLE;
TOK_LIKESCHEMA;
TOK_DESCTABLE;
TOK_DESCFUNCTION;
TOK_ALTERTABLE_PARTITION;
TOK_ALTERTABLE_RENAME;
TOK_ALTERTABLE_ADDCOLS;
TOK_ALTERTABLE_RENAMECOL;
TOK_ALTERTABLE_RENAMEPART;
TOK_ALTERTABLE_REPLACECOLS;
TOK_ALTERTABLE_ADDPARTS;
TOK_ALTERTABLE_DROPPARTS;
TOK_ALTERTABLE_ALTERPARTS_PROTECTMODE;
TOK_ALTERTABLE_TOUCH;
TOK_ALTERTABLE_ARCHIVE;
TOK_ALTERTABLE_UNARCHIVE;
TOK_ALTERTABLE_SERDEPROPERTIES;
TOK_ALTERTABLE_SERIALIZER;
TOK_TABLE_PARTITION;
TOK_ALTERTABLE_FILEFORMAT;
TOK_ALTERTABLE_LOCATION;
TOK_ALTERTABLE_PROPERTIES;
TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION;
TOK_ALTERINDEX_REBUILD;
TOK_ALTERINDEX_PROPERTIES;
TOK_MSCK;
TOK_SHOWDATABASES;
TOK_SHOWTABLES;
TOK_SHOWCOLUMNS;
TOK_SHOWFUNCTIONS;
TOK_SHOWPARTITIONS;
TOK_SHOW_CREATETABLE;
TOK_SHOW_TABLESTATUS;
TOK_SHOW_TBLPROPERTIES;
TOK_SHOWLOCKS;
TOK_LOCKTABLE;
TOK_UNLOCKTABLE;
TOK_SWITCHDATABASE;
TOK_DROPDATABASE;
TOK_DROPTABLE;
TOK_DATABASECOMMENT;
TOK_TABCOLLIST;
TOK_TABCOL;
TOK_TABLECOMMENT;
TOK_TABLEPARTCOLS;
TOK_TABLEBUCKETS;
TOK_TABLEROWFORMAT;
TOK_TABLEROWFORMATFIELD;
TOK_TABLEROWFORMATCOLLITEMS;
TOK_TABLEROWFORMATMAPKEYS;
TOK_TABLEROWFORMATLINES;
TOK_TBLSEQUENCEFILE;
TOK_TBLTEXTFILE;
TOK_TBLRCFILE;
TOK_TABLEFILEFORMAT;
TOK_FILEFORMAT_GENERIC;
TOK_OFFLINE;
TOK_ENABLE;
TOK_DISABLE;
TOK_READONLY;
TOK_NO_DROP;
TOK_STORAGEHANDLER;
TOK_ALTERTABLE_CLUSTER_SORT;
TOK_TABCOLNAME;
TOK_TABLELOCATION;
TOK_PARTITIONLOCATION;
TOK_TABLEBUCKETSAMPLE;
TOK_TABLESPLITSAMPLE;
TOK_TMP_FILE;
TOK_TABSORTCOLNAMEASC;
TOK_TABSORTCOLNAMEDESC;
TOK_STRINGLITERALSEQUENCE;
TOK_CHARSETLITERAL;
TOK_CREATEFUNCTION;
TOK_DROPFUNCTION;
TOK_CREATEVIEW;
TOK_DROPVIEW;
TOK_ALTERVIEW_PROPERTIES;
TOK_ALTERVIEW_ADDPARTS;
TOK_ALTERVIEW_DROPPARTS;
TOK_ALTERVIEW_RENAME;
TOK_VIEWPARTCOLS;
TOK_EXPLAIN;
TOK_TABLESERIALIZER;
TOK_TABLEPROPERTIES;
TOK_TABLEPROPLIST;
TOK_INDEXPROPERTIES;
TOK_INDEXPROPLIST;
TOK_TABTYPE;
TOK_LIMIT;
TOK_TABLEPROPERTY;
TOK_IFEXISTS;
TOK_IFNOTEXISTS;
TOK_ORREPLACE;
TOK_HINTLIST;
TOK_HINT;
TOK_MAPJOIN;
TOK_STREAMTABLE;
TOK_HOLD_DDLTIME;
TOK_HINTARGLIST;
TOK_USERSCRIPTCOLNAMES;
TOK_USERSCRIPTCOLSCHEMA;
TOK_RECORDREADER;
TOK_RECORDWRITER;
TOK_LEFTSEMIJOIN;
TOK_LATERAL_VIEW;
TOK_TABALIAS;
TOK_ANALYZE;
//tokens for authentification and authorization by liulichao, begin 
TOK_DBA;
//TOK_PRIV_DELETE;	//to discuss whether it should exist.
//TOK_PRIV_INSERT;	//to discuss whether it should exist.
//TOK_PRIV_SHOWVIEW;
//TOK_PRIV_CREATEVIEW;
TOK_CREATEUSER;	//authentification_1
TOK_DROPUSER;	//authentification_2
TOK_SHOW_USERNAMES;	//The privilege of DBAs
TOK_CHANGE_PWD;	//authentification_3
TOK_AUTHENTICATION;	//authentification_4
//TOK_SWITCHUSER;
//tokens for authentification and authorization by liulichao, end
TOK_CREATEROLE;
TOK_DROPROLE;
TOK_GRANT;
TOK_REVOKE;
TOK_SHOW_GRANT;
TOK_PRIVILEGE_LIST;
TOK_PRIVILEGE;
TOK_PRINCIPAL_NAME;
TOK_USER;
TOK_GROUP;
TOK_ROLE;
TOK_GRANT_WITH_OPTION;
TOK_CREATE_AS_DBA;	//added by liulichao
TOK_PRIV_DBA;		//added by liulichao
TOK_PRIV_ALL;
TOK_PRIV_ALTER_METADATA;
TOK_PRIV_ALTER_DATA;
TOK_PRIV_DROP;
TOK_PRIV_INDEX;
TOK_PRIV_LOCK;
TOK_PRIV_SELECT;
TOK_PRIV_SHOW_DATABASE;
TOK_PRIV_CREATE;
TOK_PRIV_OBJECT;
TOK_PRIV_OBJECT_COL;
TOK_GRANT_ROLE;
TOK_REVOKE_ROLE;
TOK_SHOW_ROLE_GRANT;
TOK_SHOWINDEXES;
TOK_INDEXCOMMENT;
TOK_DESCDATABASE;
TOK_DATABASEPROPERTIES;
TOK_DATABASELOCATION;
TOK_DBPROPLIST;
TOK_ALTERDATABASE_PROPERTIES;
TOK_ALTERTABLE_ALTERPARTS_MERGEFILES;
TOK_TABNAME;
TOK_TABSRC;
TOK_RESTRICT;
TOK_CASCADE;
TOK_TABLESKEWED;
TOK_TABCOLVALUE;
TOK_TABCOLVALUE_PAIR;
TOK_TABCOLVALUES;
TOK_ALTERTABLE_SKEWED;
TOK_ALTERTBLPART_SKEWED_LOCATION;
TOK_SKEWED_LOCATIONS;
TOK_SKEWED_LOCATION_LIST;
TOK_SKEWED_LOCATION_MAP;
TOK_STOREDASDIRS;
TOK_SPLITED_BY;
TOK_SUBSPLITED_BY;
TOK_SUBSPLIT_EXPER;
TOK_SPLIT;
TOK_SPLIT_EXPER;
TOK_SUBSPLIT;
TOK_PARTITIONED_BY;
TOK_SUBPARTITIONED_BY;
TOK_PARTITION_EXPER;
TOK_SUBPARTITION_EXPER;
TOK_PARTITION;
TOK_SUBPARTITION;
TOK_STR_OR_NUM_OR_FUNC;
TOK_ALTER_DW;
TOK_VALUES_LESS;
TOK_VALUES_GREATER;
TOK_VALUES;

TOK_ADDNODE;
TOK_DROPNODE;
TOK_NODERPROPERTIES;
TOK_NODEPROPLIST;
TOK_MODIFYNODE;
TOK_ALTERTABLE_DROP_PARTITION;
TOK_ALTERTABLE_DROP_SUBPARTITION;
TOK_ALTERTABLE_ADD_PARTITION;
TOK_ALTERTABLE_ADD_SUBPARTITION;
TOK_ALTERTABLE_MODIFY_PARTITION_ADD_FILE;
TOK_ALTERTABLE_MODIFY_PARTITION_DROP_FILE;
TOK_ALTERTABLE_MODIFY_SUBPARTITION_ADD_FILE;
TOK_ALTERTABLE_MODIFY_SUBPARTITION_DROP_FILE;
TOK_ALTERINDEX_DROP_PARTINDEXS;
TOK_ALTERINDEX_DROP_SUBPARTINDEXS;
TOK_ALTERINDEX_ADD_PARTINDEXS;
TOK_ALTERINDEX_ADD_SUBPARTINDEXS;
TOK_ALTERINDEX_MODIFY_PARTITION_ADD_FILE;
TOK_ALTERINDEX_MODIFY_SUBPARTITION_ADD_FILE;
TOK_ALTERINDEX_MODIFY_PARTINDEX_DROP_FILE;
TOK_ALTERINDEX_MODIFY_SUBPARTINDEX_DROP_FILE;
TOK_SHOWSUBPARTITIONS;
TOK_HETER;
TOK_SHOWPARTITIONKEYS;
TOK_SHOWBUSITYPES;
TOK_BUSITYPECOMMENT;
TOK_CREATEBUSITYPE;
TOK_SHOWNODES;
TOK_SHOWFILES;
TOK_SHOWFILELOCATIONS;
TOK_STRINGLITERALLIST;
TOK_TABLEDISTRIBUTION;

TOK_SCHEMANAME;
TOK_CREATESCHEMA;
TOK_DROPSCHEMA;

TOK_CREATENODEGROUP;
TOK_ALTER_NODEGROUP_ADD_NODES;
TOK_ALTER_NODEGROUP_DELETE_NODES;
TOK_NODEGROUPPROPERTIES;
TOK_MODIFYNODEGROUP;
TOK_DROPNODEGROUP;
TOK_NODEGROUPCOMMENT;
TOK_SHOWNODEGROUPS;

TOK_CREATEGEOLOC;
TOK_DROPGEOLOC;
TOK_MODIFYGEOLOC;
TOK_SHOWGEOLOC;
TOK_CREATEEQROOM;
TOK_SHOWEQROOM;
TOK_MODIFYEQROOM;
TOK_DROPEQROOM;
TOK_CREATENODEASSIGNMENT;
TOK_DROPNODEASSIGNMENT;
TOK_ALTERSCHEMA_RENAME;
TOK_ALTERSCHEMA_ADDCOLS;
TOK_ALTERSCHEMA_REPLACECOLS;
TOK_ALTERSCHEMA_RENAMECOL;
TOK_ALTERSCHEMA_CHANGECOL_AFTER_POSITION;
TOK_ALTERSCHEMA_PROPERTIES;
TOK_SHOWNODEASSIGNMENT;
TOK_CREATENODEGROUPASSIGNMENT;
TOK_DROPNODEGROUPASSIGNMENT;
TOK_SHOWNODEGROUPASSIGNMENT;
TOK_CREATEUSERASSIGNMENT;
TOK_DROPUSERASSIGNMENT;
TOK_SHOWUSERASSIGNMENT;
TOK_CREATEROLEASSIGNMENT;
TOK_DROPROLEASSIGNMENT;
TOK_SHOWROLEASSIGNMENT;
TOK_ALTERTABLE_FILESPLIT;
TOK_ALTERTABLE_ADD_DISTRIBUTION;
TOK_ALTERTABLE_DELETE_DISTRIBUTION;
TOK_SHOWSCHEMAS;
TOK_DESCSCHEMA;

}


// Package headers
@header {
package org.apache.hadoop.hive.ql.parse;
}
@lexer::header {package org.apache.hadoop.hive.ql.parse;}


@members {
  Stack msgs = new Stack<String>();
}

@rulecatch {
catch (RecognitionException e) {
 reportError(e);
  throw e;
}
}

// starting rule
statement
	: explainStatement EOF
	| execStatement EOF
	;

explainStatement
@init { msgs.push("explain statement"); }
@after { msgs.pop(); }
	: KW_EXPLAIN (explainOptions=KW_EXTENDED|explainOptions=KW_FORMATTED|explainOptions=KW_DEPENDENCY)? execStatement
      -> ^(TOK_EXPLAIN execStatement $explainOptions?)
	;

execStatement
@init { msgs.push("statement"); }
@after { msgs.pop(); }
    : queryStatementExpression
    | loadStatement
    | exportStatement
    | importStatement
    | ddlStatement
    ;

loadStatement
@init { msgs.push("load statement"); }
@after { msgs.pop(); }
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tableOrPartition)
    -> ^(TOK_LOAD $path $tab $islocal? $isoverwrite?)
    ;

exportStatement
@init { msgs.push("export statement"); }
@after { msgs.pop(); }
    : KW_EXPORT KW_TABLE (tab=tableOrPartition) KW_TO (path=StringLiteral)
    -> ^(TOK_EXPORT $tab $path)
    ;

importStatement
@init { msgs.push("import statement"); }
@after { msgs.pop(); }
	: KW_IMPORT ((ext=KW_EXTERNAL)? KW_TABLE (tab=tableOrPartition))? KW_FROM (path=StringLiteral) tableLocation?
    -> ^(TOK_IMPORT $path $tab? $ext? tableLocation?)
    ;

ddlStatement
@init { msgs.push("ddl statement"); }
@after { msgs.pop(); }
    : createDatabaseStatement
    | switchDatabaseStatement
    | dropDatabaseStatement
    | createTableStatement
    | dropTableStatement
    | alterStatement
    | descStatement
    | showStatement
    | metastoreCheck
    | createViewStatement
    | dropViewStatement
    | createFunctionStatement
    | createIndexStatement
    | dropIndexStatement
    | dropFunctionStatement
    | analyzeStatement
    | lockStatement
    | unlockStatement
//    | createRoleStatement
//    | dropRoleStatement
    | grantPrivileges
    | revokePrivileges
    | showGrants
    | showRoleGrants
    | grantRole
    | revokeRole
    | createUserStatement	//added by liulichao,begin
    | dropUserStatement
    | showUserNames
    | changePassword
//    | switchUserStatement
    | authentificationStatement//added by liulichao,end
    | datawarehouseStatement
    | addNodeStatement
    | dropNodeStatement
    | alterNodeStatement
    | createBusitypeStatement
    | createGeoLocStatement
    | dropGeoLocStatement
    | alterGeoLocStatement
    | showGeoLoc
    | createEqRoomStatement
    | dropEqRoomStatement
    | alterEqRoomStatement
    | showEqRoom
    | createNodeAssignmentStatement
    | dropNodeAssignmentStatement
    | showNodeAssignment
    | createNodeGroupAssignmentStatement
    | dropNodeGroupAssignmentStatement
    | showNodeGroupAssignment
    | createRoleStatement
    | dropRoleStatement
    | createUserAssignmentStatement
    | dropUserAssignmentStatement
    | showUserAssignment
    | createRoleAssignmentStatement
    | dropRoleAssignmentStatement
    | showRoleAssignment
    
    | createNodeGroupStatement
    | alterNodeGroupStatement
    | modifyNodeGroupStatement
    | dropNodeGroupStatement
    
    |createSchemaStatement
    |dropSchemaStatement
    
    ;
//

showRoleAssignment
@init { msgs.push("show RoleAssignment"); }
@after { msgs.pop(); }
    :  KW_SHOW KW_ROLEASSIGNMENT
     -> ^(TOK_SHOWROLEASSIGNMENT)
    ;
dropRoleAssignmentStatement
@init { msgs.push("drop RoleAssignmentStatement"); }
@after { msgs.pop(); }
    : KW_DROP KW_ROLEASSIGNMENT LPAREN DBNAME= StringLiteral COMMA ROLE_NAME=StringLiteral RPAREN 
    -> ^(TOK_DROPROLEASSIGNMENT $DBNAME $ROLE_NAME)
    ;
createRoleAssignmentStatement
@init { msgs.push("create RoleAssignmentStatement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_ROLEASSIGNMENT LPAREN DBNAME= StringLiteral COMMA ROLE_NAME=StringLiteral RPAREN 
    -> ^(TOK_CREATEROLEASSIGNMENT $DBNAME $ROLE_NAME)
    ;       
showUserAssignment
@init { msgs.push("show UserAssignment"); }
@after { msgs.pop(); }
    :  KW_SHOW KW_USERASSIGNMENT
     -> ^(TOK_SHOWUSERASSIGNMENT)
    ;
dropUserAssignmentStatement
@init { msgs.push("drop UserAssignmentStatement"); }
@after { msgs.pop(); }
    : KW_DROP KW_USERASSIGNMENT LPAREN DBNAME= StringLiteral COMMA USER_NAME=StringLiteral RPAREN 
    -> ^(TOK_DROPUSERASSIGNMENT $DBNAME $USER_NAME)
    ;
createUserAssignmentStatement
@init { msgs.push("create UserAssignmentStatement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_USERASSIGNMENT LPAREN DBNAME= StringLiteral COMMA USER_NAME=StringLiteral RPAREN 
    -> ^(TOK_CREATEUSERASSIGNMENT $DBNAME $USER_NAME)
    ;       
showNodeGroupAssignment
@init { msgs.push("show NodeGroupAssignment"); }
@after { msgs.pop(); }
    :  KW_SHOW KW_NODEGROUPASSIGNMENT
     -> ^(TOK_SHOWNODEGROUPASSIGNMENT)
    ;
dropNodeGroupAssignmentStatement
@init { msgs.push("drop NodeGroupAssignmentStatement"); }
@after { msgs.pop(); }
    : KW_DROP KW_NODEGROUPASSIGNMENT LPAREN DBNAME= StringLiteral COMMA NODEGROUP_NAME=StringLiteral RPAREN 
    -> ^(TOK_DROPNODEGROUPASSIGNMENT $DBNAME $NODEGROUP_NAME)
    ;
createNodeGroupAssignmentStatement
@init { msgs.push("create NodeGroupAssignmentStatement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_NODEGROUPASSIGNMENT LPAREN DBNAME= StringLiteral COMMA NODEGROUP_NAME=StringLiteral RPAREN 
    -> ^(TOK_CREATENODEGROUPASSIGNMENT $DBNAME $NODEGROUP_NAME)
    ;       
showNodeAssignment
@init { msgs.push("show NodeAssignment"); }
@after { msgs.pop(); }
    :  KW_SHOW KW_NODEASSIGNMENT
     -> ^(TOK_SHOWNODEASSIGNMENT)
    ;       
dropNodeAssignmentStatement
@init { msgs.push("drop NodeAssignmentStatement"); }
@after { msgs.pop(); }
    : KW_DROP KW_NODEASSIGNMENT LPAREN NODE_NAME=StringLiteral COMMA DBNAME= StringLiteral RPAREN 
    -> ^(TOK_DROPNODEASSIGNMENT $NODE_NAME $DBNAME)
    ; 
createNodeAssignmentStatement
@init { msgs.push("create NodeAssignmentStatement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_NODEASSIGNMENT LPAREN NODE_NAME=StringLiteral COMMA DBNAME= StringLiteral RPAREN 
    -> ^(TOK_CREATENODEASSIGNMENT $NODE_NAME $DBNAME)
    ;
showEqRoom
@init { msgs.push("show EqRoom"); }
@after { msgs.pop(); }
    :  KW_SHOW KW_EQROOM
     -> ^(TOK_SHOWEQROOM)
    ;
alterEqRoomStatement
@init { msgs.push("alter EqRoomStatement"); }
@after { msgs.pop(); }
    :  KW_MODIFY KW_EQROOM LPAREN EQ_ROOM_NAME=StringLiteral COMMA STATUS= Identifier RPAREN  (KW_COMMENT CMT=StringLiteral)? ( KW_ON GEO_LOC_NAME=StringLiteral)? 
    -> ^(TOK_MODIFYEQROOM $EQ_ROOM_NAME $STATUS $CMT? (KW_ON $GEO_LOC_NAME)?)
    ;
    
dropEqRoomStatement
@init { msgs.push("drop EqRoomStatement"); }
@after { msgs.pop(); }
    : KW_DROP KW_EQROOM StringLiteral 
    -> ^(TOK_DROPEQROOM StringLiteral)
    ;
createEqRoomStatement
@init { msgs.push("create EqRoomStatement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_EQROOM LPAREN EQ_ROOM_NAME=StringLiteral COMMA STATUS= Identifier RPAREN  (KW_COMMENT CMT=StringLiteral)? ( KW_ON GEO_LOC_NAME=StringLiteral)? 
    -> ^(TOK_CREATEEQROOM $EQ_ROOM_NAME $STATUS $CMT? (KW_ON $GEO_LOC_NAME)? )
    ;
        
showGeoLoc
@init { msgs.push("show GeoLoc"); }
@after { msgs.pop(); }
    :  KW_SHOW KW_GEOLOC
     -> ^(TOK_SHOWGEOLOC)
    ;
alterGeoLocStatement
@init { msgs.push("alter GeoLocstatement"); }
@after { msgs.pop(); }
    :  KW_MODIFY KW_GEOLOC LPAREN GEO_LOC_NAME=StringLiteral COMMA NATION=StringLiteral COMMA PROVINCE=StringLiteral COMMA CITY= StringLiteral COMMA DIST=StringLiteral RPAREN 
    -> ^(TOK_MODIFYGEOLOC $GEO_LOC_NAME $NATION $PROVINCE $CITY $DIST)
    ;
dropGeoLocStatement
@init { msgs.push("drop GeoLocStatement"); }
@after { msgs.pop(); }
    : KW_DROP KW_GEOLOC StringLiteral 
    -> ^(TOK_DROPGEOLOC StringLiteral)
    ;
createGeoLocStatement
@init { msgs.push("create GeoLocStatement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_GEOLOC LPAREN  GEO_LOC_NAME=StringLiteral COMMA NATION=StringLiteral COMMA PROVINCE=StringLiteral COMMA CITY= StringLiteral COMMA DIST=StringLiteral RPAREN 
    -> ^(TOK_CREATEGEOLOC $GEO_LOC_NAME $NATION $PROVINCE $CITY $DIST)
        ;
ifExists
@init { msgs.push("if exists clause"); }
@after { msgs.pop(); }
    : KW_IF KW_EXISTS
    -> ^(TOK_IFEXISTS)
    ;

restrictOrCascade
@init { msgs.push("restrict or cascade clause"); }
@after { msgs.pop(); }
    : KW_RESTRICT
    -> ^(TOK_RESTRICT)
    | KW_CASCADE
    -> ^(TOK_CASCADE)
    ;

ifNotExists
@init { msgs.push("if not exists clause"); }
@after { msgs.pop(); }
    : KW_IF KW_NOT KW_EXISTS
    -> ^(TOK_IFNOTEXISTS)
    ;

storedAsDirs
@init { msgs.push("stored as directories"); }
@after { msgs.pop(); }
    : KW_STORED KW_AS KW_DIRECTORIES
    -> ^(TOK_STOREDASDIRS)
    ;
    
orReplace
@init { msgs.push("or replace clause"); }
@after { msgs.pop(); }
    : KW_OR KW_REPLACE
    -> ^(TOK_ORREPLACE)
    ;
    
//begin of nodegroup

createNodeGroupStatement
@init { msgs.push("create nodegroup statement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_NODEGROUP
        ifNotExists?
        name=Identifier
        nodegroupComment?
        (KW_WITH KW_DBPROPERTIES nodegroupprops=nodegroupProperties)?
        (KW_ON KW_NODES nodes=stringLiteralList)?
    -> ^(TOK_CREATENODEGROUP $name ifNotExists?  nodegroupComment? $nodegroupprops? $nodes?)
    ;

alterNodeGroupStatement
@init { msgs.push("alter nodegroup statement"); }
@after { msgs.pop(); }
    : KW_ALTER KW_NODEGROUP
        name=Identifier
        nodegroupComment?
        (KW_WITH KW_DBPROPERTIES nodegroupprops=nodegroupProperties)?
        ( (add=KW_ADD | delete=KW_DELETE) KW_NODES nodes=stringLiteralList)?
    -> {$add != null}? ^(TOK_ALTER_NODEGROUP_ADD_NODES $name nodegroupComment? $nodegroupprops? $nodes?)
    -> ^(TOK_ALTER_NODEGROUP_DELETE_NODES $name nodegroupComment? $nodegroupprops? $nodes?)
    ;   
    
nodegroupProperties
@init { msgs.push("nodegroupproperties"); }
@after { msgs.pop(); }
    :
      LPAREN nodegroupPropertiesList RPAREN -> ^(TOK_NODEGROUPPROPERTIES nodegroupPropertiesList)
    ;
    
nodegroupPropertiesList
@init { msgs.push("nodegroup properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_NODEGROUPPROPERTIES keyValueProperty+)
    ;

modifyNodeGroupStatement
@init { msgs.push("modify nodegroup statement"); }
@after { msgs.pop(); }
    : KW_USE KW_NODEGROUP Identifier
    -> ^(TOK_MODIFYNODEGROUP Identifier)
    ;

dropNodeGroupStatement
@init { msgs.push("drop nodegroup statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_NODEGROUP ifExists? Identifier restrictOrCascade?
    -> ^(TOK_DROPNODEGROUP Identifier ifExists? restrictOrCascade?)
    ;

nodegroupComment
@init { msgs.push("nodegroup's comment"); }
@after { msgs.pop(); }
    : KW_COMMENT comment=StringLiteral
    -> ^(TOK_NODEGROUPCOMMENT $comment)
    ;

   
//end of nodegroup

//FIXME
createBusitypeStatement
@init { msgs.push("create busiType statement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_BUSITYPE
        name=Identifier
        busitypeComment?
    -> ^(TOK_CREATEBUSITYPE $name busitypeComment?)
    ;
    
busitypeComment
@init { msgs.push("busitype's comment"); }
@after { msgs.pop(); }
    : KW_COMMENT comment=StringLiteral
    -> ^(TOK_BUSITYPECOMMENT $comment)
    ;



//start of node


addNodeStatement
@init { msgs.push("add node statement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_NODE LPAREN name=Identifier COMMA status=StringLiteral COMMA ip=StringLiteral RPAREN 
        (KW_WITH KW_NODEPROPERTIES nodeprops=nodeProperties)?
    -> ^(TOK_ADDNODE $name $status $ip $nodeprops?)
        ;
nodeProperties
@init { msgs.push("nodeproperties"); }
@after { msgs.pop(); }
    :
      LPAREN nodePropertiesList RPAREN -> ^(TOK_NODERPROPERTIES nodePropertiesList)
    ;
nodePropertiesList
@init { msgs.push("node properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_NODEPROPLIST keyValueProperty+)
    ;

dropNodeStatement
@init { msgs.push("drop node statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_NODE Identifier 
    -> ^(TOK_DROPNODE Identifier)
    ;
    
    
alterNodeStatement
@init { msgs.push("alter node properties statement"); }
@after { msgs.pop(); }
    :  KW_MODIFY KW_NODE LPAREN name=Identifier COMMA status=Identifier COMMA ip=Identifier RPAREN 
        (KW_WITH KW_NODEPROPERTIES nodeprops=nodeProperties)?
    -> ^(TOK_MODIFYNODE $name $status $ip $nodeprops?)
    ;
//end of node


createDatabaseStatement
@init { msgs.push("create database statement"); }
@after { msgs.pop(); }
    : KW_CREATE  KW_DATABASE
        ifNotExists?
        name=Identifier
        databaseComment?
        dbLocation?
        (KW_WITH KW_DBPROPERTIES dbprops=dbProperties)?
    -> ^(TOK_CREATEDATABASE $name ifNotExists? dbLocation? databaseComment? $dbprops?)
    ;

dbLocation
@init { msgs.push("database location specification"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_DATABASELOCATION $locn)
    ;

dbProperties
@init { msgs.push("dbproperties"); }
@after { msgs.pop(); }
    :
      LPAREN dbPropertiesList RPAREN -> ^(TOK_DATABASEPROPERTIES dbPropertiesList)
    ;

dbPropertiesList
@init { msgs.push("database properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_DBPROPLIST keyValueProperty+)
    ;


switchDatabaseStatement
@init { msgs.push("switch database statement"); }
@after { msgs.pop(); }
    : KW_USE Identifier
    -> ^(TOK_SWITCHDATABASE Identifier)
    ;

dropDatabaseStatement
@init { msgs.push("drop database statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_DATABASE ifExists? Identifier restrictOrCascade?
    -> ^(TOK_DROPDATABASE Identifier ifExists? restrictOrCascade?)
    ;

databaseComment
@init { msgs.push("database's comment"); }
@after { msgs.pop(); }
    : KW_COMMENT comment=StringLiteral
    -> ^(TOK_DATABASECOMMENT $comment)
    ;

//reuse table definition fragments
createSchemaStatement
@init { msgs.push("create table statement"); }
@after { msgs.pop(); }
    : 
    /*
    KW_CREATE KW_SCHEMA ifNotExists? name=schemaName
         (LPAREN columnNameTypeList RPAREN)
         tableComment?
         tablePropertiesPrefixed?
    -> ^(TOK_CREATESCHEMA $name ifNotExists? columnNameTypeList
         tableComment?
         tablePropertiesPrefixed?
        )
    |
      KW_CREATE KW_SCHEMA ifNotExists? name=schemaName
         (like=KW_LIKE likeName=schemaName) tableComment?
         tablePropertiesPrefixed?
    -> ^(TOK_CREATESCHEMA $name ifNotExists?
         TOK_LIKESCHEMA $likeName
         tableComment?
         tablePropertiesPrefixed?
        )
    */
    
      KW_CREATE KW_SCHEMA ifNotExists? name=schemaName
         (
          like=KW_LIKE KW_SCHEMA likeName=schemaName
          |
          LPAREN columnNameTypeList RPAREN
         )
         tableComment?
         schemaPropertiesPrefixed?
    -> ^(TOK_CREATESCHEMA $name ifNotExists?
         ^(TOK_LIKESCHEMA $likeName?)
         columnNameTypeList?
         tableComment?
         schemaPropertiesPrefixed?
         )
    ;
 
createTableStatement
@init { msgs.push("create table statement"); }
@after { msgs.pop(); }
    : 
    /*
    KW_CREATE (ext=KW_EXTERNAL)? KW_TABLE ifNotExists? name=tableName
      (  like=KW_LIKE likeTabName=tableName
         tableLocation?
       | (LPAREN columnNameTypeList RPAREN)?
         tableComment?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         (KW_AS selectStatement)?
      )
    -> ^(TOK_CREATETABLE $name $ext? ifNotExists?
         ^(TOK_LIKETABLE $likeTabName?)
         columnNameTypeList?
         tableComment?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         selectStatement?
        )
    |KW_CREATE KW_TABLE ifNotExists? tname=tableName
      (  like=KW_LIKE KW_SCHEMA likeName=schemaName KW_TO dbName=Identifier
         tableLocation?
         tableComment?
         tablePartition?
         tableDistribution?
         tablePropertiesPrefixed?
      )
    -> ^(TOK_CREATETABLE ifNotExists? $tname
         ^(TOK_LIKESCHEMA $likeName $dbName)
         tableComment?
         tablePartition?
         tableDistribution?
         tablePropertiesPrefixed?
        )
        */
         
        KW_CREATE (ext=KW_EXTERNAL)? KW_TABLE ifNotExists? name=tableName
      (  like=KW_LIKE (KW_TABLE likeTabName=tableName |KW_SCHEMA likeName=schemaName KW_TO dbName=Identifier)
         tableComment?  fileSplit? tablePartition?  tableDistribution?
       | (LPAREN columnNameTypeList RPAREN)?
         tableComment?
         fileSplit?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         tableDistribution?
         (KW_AS selectStatement)?
      )
    -> ^(TOK_CREATETABLE $name $ext? ifNotExists?
         ^(TOK_LIKETABLE $likeTabName? )
         ^(TOK_LIKESCHEMA $likeName? $dbName?)
         columnNameTypeList?
         tableComment?
         fileSplit?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         tableDistribution?
         selectStatement?
        )
    ;

createIndexStatement
@init { msgs.push("create index statement");}
@after {msgs.pop();}
    : KW_CREATE KW_INDEX indexName=Identifier
      KW_ON KW_TABLE tab=tableName LPAREN indexedCols=columnNameList RPAREN
      KW_AS typeName=StringLiteral
      autoRebuild?
      indexPropertiesPrefixed?
      indexTblName?
      tableRowFormat?
      tableFileFormat?
      tableLocation?
      tablePropertiesPrefixed?
      indexComment?
    ->^(TOK_CREATEINDEX $indexName $typeName $tab $indexedCols
        autoRebuild?
        indexPropertiesPrefixed?
        indexTblName?
        tableRowFormat?
        tableFileFormat?
        tableLocation?
        tablePropertiesPrefixed?
        indexComment?)
    ;

indexComment
@init { msgs.push("comment on an index");}
@after {msgs.pop();}
        :
                KW_COMMENT comment=StringLiteral  -> ^(TOK_INDEXCOMMENT $comment)
        ;

autoRebuild
@init { msgs.push("auto rebuild index");}
@after {msgs.pop();}
    : KW_WITH KW_DEFERRED KW_REBUILD
    ->^(TOK_DEFERRED_REBUILDINDEX)
    ;

indexTblName
@init { msgs.push("index table name");}
@after {msgs.pop();}
    : KW_IN KW_TABLE indexTbl=tableName
    ->^(TOK_CREATEINDEX_INDEXTBLNAME $indexTbl)
    ;

indexPropertiesPrefixed
@init { msgs.push("table properties with prefix"); }
@after { msgs.pop(); }
    :
        KW_IDXPROPERTIES! indexProperties
    ;

indexProperties
@init { msgs.push("index properties"); }
@after { msgs.pop(); }
    :
      LPAREN indexPropertiesList RPAREN -> ^(TOK_INDEXPROPERTIES indexPropertiesList)
    ;

indexPropertiesList
@init { msgs.push("index properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_INDEXPROPLIST keyValueProperty+)
    ;

dropIndexStatement
@init { msgs.push("drop index statement");}
@after {msgs.pop();}
    : KW_DROP KW_INDEX ifExists? indexName=Identifier KW_ON tab=tableName
    ->^(TOK_DROPINDEX $indexName $tab ifExists?)
    ;
dropSchemaStatement
@init { msgs.push("drop schema statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_SCHEMA ifExists? schemaName -> ^(TOK_DROPSCHEMA schemaName ifExists?)
    ;
    
dropTableStatement
@init { msgs.push("drop statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_TABLE ifExists? tableName -> ^(TOK_DROPTABLE tableName ifExists?)
    ;

alterStatement
@init { msgs.push("alter statement"); }
@after { msgs.pop(); }
    : KW_ALTER!
        (
            KW_TABLE! alterTableStatementSuffix
        |
            KW_VIEW! alterViewStatementSuffix
        |
            KW_INDEX! alterIndexStatementSuffix
        |
            KW_DATABASE! alterDatabaseStatementSuffix
        |
            KW_SCHEMA! alterSchemaStatementSuffix
        )
    ;


alterSchemaStatementSuffix
@init { msgs.push("alter Schema statement"); }
@after { msgs.pop(); }
    : alterSchemaStatementSuffixRename
    | alterSchemaStatementSuffixAddCol
    | alterSchemaStatementSuffixRenameCol
    | alterSchemaStatementSuffixProperties
    |alterSchemaStatementChangeColPosition
    ;
    
alterTableStatementSuffix
@init { msgs.push("alter table statement"); }
@after { msgs.pop(); }
    : alterStatementSuffixRename
    | alterStatementSuffixAddCol
    | alterStatementSuffixRenameCol
    | alterStatementSuffixFileSplit
    | alterStatementSuffixDistribution
    | alterStatementSuffixDropPartitions
    | alterStatementSuffixAddPartitions
    | alterStatementSuffixModiFyPartitionDropFiles
    | alterStatementSuffixModiFyPartitionADDFiles
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | alterTblPartitionStatement
    | alterStatementSuffixClusterbySortby
    | alterStatementSuffixSkewedby
    ;

alterViewStatementSuffix
@init { msgs.push("alter view statement"); }
@after { msgs.pop(); }
    : alterViewSuffixProperties
    | alterStatementSuffixRename
        -> ^(TOK_ALTERVIEW_RENAME alterStatementSuffixRename)
    | alterStatementSuffixAddPartitions
        -> ^(TOK_ALTERVIEW_ADDPARTS alterStatementSuffixAddPartitions)
    | alterStatementSuffixDropPartitions
        -> ^(TOK_ALTERVIEW_DROPPARTS alterStatementSuffixDropPartitions)
    ;

alterIndexStatementSuffix
@init { msgs.push("alter index statement"); }
@after { msgs.pop(); }
    : indexName=Identifier
      (KW_ON tableNameId=Identifier)
      partitionSpec?
    (
      KW_REBUILD
      ->^(TOK_ALTERINDEX_REBUILD $tableNameId $indexName partitionSpec?)
    |
      KW_SET KW_IDXPROPERTIES
      indexProperties
      ->^(TOK_ALTERINDEX_PROPERTIES $tableNameId $indexName indexProperties)
      
    | KW_DROP KW_PARTITION partition_name=Identifier  
    -> ^(TOK_ALTERINDEX_DROP_PARTINDEXS $tableNameId $partition_name )
    |KW_DROP KW_SUBPARTITION partition_name=Identifier 
    -> ^(TOK_ALTERINDEX_DROP_SUBPARTINDEXS $tableNameId $partition_name )
    | KW_ADD KW_PARTITION partition_name=Identifier 
    -> ^(TOK_ALTERINDEX_ADD_PARTINDEXS $tableNameId $partition_name )
    |KW_ADD KW_SUBPARTITION partition_name=Identifier   
    -> ^(TOK_ALTERINDEX_ADD_SUBPARTINDEXS $tableNameId $partition_name)
    | KW_ADD KW_PARTITION KW_ADD partition_name=Identifier  KW_ADD KW_FILE file_id=Identifier
    -> ^(TOK_ALTERINDEX_MODIFY_PARTITION_ADD_FILE $tableNameId $partition_name $file_id)
    |KW_MODIFY KW_SUBPARTITION partition_name=Identifier  KW_ADD KW_FILE file_id=Identifier
    -> ^(TOK_ALTERINDEX_MODIFY_SUBPARTITION_ADD_FILE $tableNameId $partition_name $file_id)
    | KW_ON KW_PARTITION partition_name=Identifier  KW_DROP KW_FILE file_id=Identifier
    -> ^(TOK_ALTERINDEX_MODIFY_PARTINDEX_DROP_FILE $tableNameId $partition_name $file_id)
    |KW_ON KW_SUBPARTITION partition_name=Identifier KW_DROP KW_FILE file_id=Identifier
    -> ^(TOK_ALTERINDEX_MODIFY_SUBPARTINDEX_DROP_FILE $tableNameId $partition_name $file_id)
      
    /*
    | alterStatementSuffixDropPartitionIndexs
    | alterStatementSuffixAddPartitionIndexs
    | alterStatementSuffixModiFyPartitionIndexDropFiles
    | alterStatementSuffixModiFyPartitionIndexADDFiles  
    */
    )
    
    ;
/*
 alterStatementSuffixDropPartitionIndexs
 @init { msgs.push("drop partition index statement"); }
 @after { msgs.pop(); }
   :KW_DROP KW_PARTITION partition_name=Identifier  
    -> ^(TOK_ALTERINDEX_DROP_PARTINDEXS $partition_name )
    |KW_DROP KW_SUBPARTITION partition_name=Identifier 
    -> ^(TOK_ALTERINDEX_DROP_SUBPARTINDEXS $partition_name )
    ;
    
  alterStatementSuffixAddPartitionIndexs
  @init { msgs.push("add partition index statement"); }
  @after { msgs.pop(); }
   : KW_ADD KW_PARTITION partition_name=Identifier 
    -> ^(TOK_ALTERINDEX_ADD_PARTINDEXS $partition_name )
    |KW_ADD KW_SUBPARTITION partition_name=Identifier   
    -> ^(TOK_ALTERINDEX_ADD_SUBPARTINDEXS $partition_name)
    ;
    
  alterStatementSuffixModiFyPartitionIndexDropFiles
  @init { msgs.push("add partition index file statement"); }
  @after { msgs.pop(); }
   :KW_ADD KW_PARTITION KW_ADD partition_name=Identifier  KW_ADD KW_FILE file_id=Identifier
    -> ^(TOK_ALTERINDEX_MODIFY_PARTITION_ADD_FILE $partition_name $file_id)
    |KW_MODIFY KW_SUBPARTITION partition_name=Identifier  KW_ADD KW_FILE file_id=Identifier
    -> ^(TOK_ALTERINDEX_MODIFY_SUBPARTITION_ADD_FILE $partition_name $file_id)
    ;
    
  alterStatementSuffixModiFyPartitionIndexADDFiles
  @init { msgs.push("drop partition index file statement"); }
  @after { msgs.pop(); }
   : KW_ON KW_PARTITION partition_name=Identifier  KW_DROP KW_FILE file_id=Identifier
    -> ^(TOK_ALTERINDEX_MODIFY_PARTINDEX_DROP_FILE $partition_name $file_id)
    |KW_ON KW_SUBPARTITION partition_name=Identifier KW_DROP KW_FILE file_id=Identifier
    -> ^(TOK_ALTERINDEX_MODIFY_SUBPARTINDEX_DROP_FILE $partition_name $file_id)
     ;
*/

alterDatabaseStatementSuffix
@init { msgs.push("alter database statement"); }
@after { msgs.pop(); }
    : alterDatabaseSuffixProperties
    ;

alterDatabaseSuffixProperties
@init { msgs.push("alter database properties statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_DBPROPERTIES dbProperties
    -> ^(TOK_ALTERDATABASE_PROPERTIES $name dbProperties)
    ;

alterStatementSuffixRename
@init { msgs.push("rename statement"); }
@after { msgs.pop(); }
    : oldName=Identifier KW_RENAME KW_TO newName=Identifier
    -> ^(TOK_ALTERTABLE_RENAME $oldName $newName)
    ;
    
alterSchemaStatementSuffixRename   
@init { msgs.push("rename statement"); }
@after { msgs.pop(); }
    : oldName=Identifier KW_RENAME KW_TO newName=Identifier
    -> ^(TOK_ALTERSCHEMA_RENAME $oldName $newName)
    ;

alterStatementSuffixAddCol
@init { msgs.push("add column statement"); }
@after { msgs.pop(); }
    : Identifier (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN
    -> {$add != null}? ^(TOK_ALTERTABLE_ADDCOLS Identifier columnNameTypeList)
    ->                 ^(TOK_ALTERTABLE_REPLACECOLS Identifier columnNameTypeList)
    ;
alterSchemaStatementSuffixAddCol
@init { msgs.push("add Schema column statement"); }
@after { msgs.pop(); }
    : Identifier (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN
    -> {$add != null}? ^(TOK_ALTERSCHEMA_ADDCOLS Identifier columnNameTypeList)
    ->                 ^(TOK_ALTERSCHEMA_REPLACECOLS Identifier columnNameTypeList)
    ;
    
alterStatementSuffixFileSplit
@init { msgs.push("alter file split"); }
@after { msgs.pop(); }
    : Identifier fileSplit
    ->^(TOK_ALTERTABLE_FILESPLIT Identifier fileSplit)
    ;
    
alterStatementSuffixDistribution
@init { msgs.push("alter table Distribution"); }
@after { msgs.pop(); }
    : Identifier (add=KW_ADD | delete=KW_DELETE) tableDistribution
    ->{$add != null}?^(TOK_ALTERTABLE_ADD_DISTRIBUTION Identifier tableDistribution)
    -> ^(TOK_ALTERTABLE_DELETE_DISTRIBUTION Identifier tableDistribution)
    ;
    
alterStatementSuffixRenameCol
@init { msgs.push("rename column name"); }
@after { msgs.pop(); }
    : Identifier KW_CHANGE KW_COLUMN? oldName=Identifier newName=Identifier colType (KW_COMMENT comment=StringLiteral)? alterStatementChangeColPosition?
    ->^(TOK_ALTERTABLE_RENAMECOL Identifier $oldName $newName colType $comment? alterStatementChangeColPosition?)
    ;

alterSchemaStatementSuffixRenameCol
@init { msgs.push("rename Schema column name"); }
@after { msgs.pop(); }
    : Identifier KW_CHANGE KW_COLUMN? oldName=Identifier newName=Identifier colType (KW_COMMENT comment=StringLiteral)? alterStatementChangeColPosition?
    ->^(TOK_ALTERSCHEMA_RENAMECOL Identifier $oldName $newName colType $comment? alterStatementChangeColPosition?)
    ;
    
alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=Identifier
    ->{$first != null}? ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION )
    -> ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION $afterCol)
    ;
    
alterSchemaStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=Identifier
    ->{$first != null}? ^(TOK_ALTERSCHEMA_CHANGECOL_AFTER_POSITION )
    -> ^(TOK_ALTERSCHEMA_CHANGECOL_AFTER_POSITION $afterCol)
    ;

//add 2-level  partition operations


alterStatementSuffixDropPartitions
@init { msgs.push("drop partition statement"); }
@after { msgs.pop(); }
    : 
    /*
    Identifier KW_DROP ifExists? dropPartitionSpec (COMMA dropPartitionSpec)*
    -> ^(TOK_ALTERTABLE_DROPPARTS Identifier dropPartitionSpec+ ifExists?)
    | 
    */
    Identifier KW_DROP KW_PARTITION partition_name=Identifier  
    -> ^(TOK_ALTERTABLE_DROP_PARTITION  Identifier $partition_name )
    | Identifier KW_DROP KW_SUBPARTITION partition_name=Identifier 
    -> ^(TOK_ALTERTABLE_DROP_SUBPARTITION  Identifier $partition_name )
    ;
    
alterStatementSuffixAddPartitions
@init { msgs.push("add partition statement"); }
@after { msgs.pop(); }
    :
    /*
     Identifier KW_ADD ifNotExists? partitionSpec partitionLocation? (partitionSpec partitionLocation?)*
    -> ^(TOK_ALTERTABLE_ADDPARTS Identifier ifNotExists? (partitionSpec partitionLocation?)+)
    |
    */
    /*
    | Identifier KW_ADD KW_PARTITION partition_name=Identifier  partitionValuesExper  subPartitionTemplate?
    -> ^(TOK_ALTERTABLE_ADD_PARTITION Identifier $partition_name partitionValuesExper subPartitionTemplate?)
    | Identifier KW_ADD KW_SUBPARTITION partition_name=Identifier  partitionValuesExper 
    -> ^(TOK_ALTERTABLE_ADD_SUBPARTITION Identifier $partition_name partitionValuesExper)
    */
    
     Identifier KW_ADD KW_PARTITION partitionTemplate 
    -> ^(TOK_ALTERTABLE_ADD_PARTITION Identifier  partitionTemplate )
    | Identifier KW_ADD KW_SUBPARTITION subPartitionTemplate   
    -> ^(TOK_ALTERTABLE_ADD_SUBPARTITION Identifier subPartitionTemplate )
    ;
    
 alterStatementSuffixModiFyPartitionDropFiles
 @init { msgs.push("add partition file statement"); }
@after { msgs.pop(); }
    : Identifier KW_ADD KW_PARTITION KW_ADD partition_name=Identifier  KW_ADD KW_FILE file_id=Identifier
    -> ^(TOK_ALTERTABLE_MODIFY_PARTITION_ADD_FILE  Identifier $partition_name $file_id)
    | Identifier KW_MODIFY KW_SUBPARTITION partition_name=Identifier  KW_ADD KW_FILE file_id=Identifier
    -> ^(TOK_ALTERTABLE_MODIFY_SUBPARTITION_ADD_FILE  Identifier $partition_name $file_id)
    ;
    
 alterStatementSuffixModiFyPartitionADDFiles
  @init { msgs.push("drop partition file statement"); }
  @after { msgs.pop(); }
   :  Identifier KW_MODIFY KW_PARTITION partition_name=Identifier  KW_DROP KW_FILE file_id=Identifier
    -> ^(TOK_ALTERTABLE_MODIFY_PARTITION_DROP_FILE  Identifier $partition_name $file_id)
    | Identifier KW_MODIFY KW_SUBPARTITION partition_name=Identifier KW_DROP KW_FILE file_id=Identifier
    -> ^(TOK_ALTERTABLE_MODIFY_SUBPARTITION_DROP_FILE  Identifier $partition_name $file_id)
     ;
	
alterStatementSuffixTouch
@init { msgs.push("touch statement"); }
@after { msgs.pop(); }
    : Identifier KW_TOUCH (partitionSpec)*
    -> ^(TOK_ALTERTABLE_TOUCH Identifier (partitionSpec)*)
    ;

alterStatementSuffixArchive
@init { msgs.push("archive statement"); }
@after { msgs.pop(); }
    : Identifier KW_ARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_ARCHIVE Identifier (partitionSpec)*)
    ;

alterStatementSuffixUnArchive
@init { msgs.push("unarchive statement"); }
@after { msgs.pop(); }
    : Identifier KW_UNARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_UNARCHIVE Identifier (partitionSpec)*)
    ;

partitionLocation
@init { msgs.push("partition location"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_PARTITIONLOCATION $locn)
    ;


alterStatementSuffixProperties
@init { msgs.push("alter properties statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_PROPERTIES $name tableProperties)
    ;
alterSchemaStatementSuffixProperties
@init { msgs.push("alter properties statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_SCHEMAPROPERTIES tableProperties
    -> ^(TOK_ALTERSCHEMA_PROPERTIES $name tableProperties)
    ;

alterViewSuffixProperties
@init { msgs.push("alter view properties statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERVIEW_PROPERTIES $name tableProperties)
    ;

alterStatementSuffixSerdeProperties
@init { msgs.push("alter serdes statement"); }
@after { msgs.pop(); }
    : KW_SET KW_SERDE serdeName=StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    -> ^(TOK_ALTERTABLE_SERIALIZER $serdeName tableProperties?)
    | KW_SET KW_SERDEPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_SERDEPROPERTIES tableProperties)
    ;

tablePartitionPrefix
@init {msgs.push("table partition prefix");}
@after {msgs.pop();}
  :name=Identifier partitionSpec?
  ->^(TOK_TABLE_PARTITION $name partitionSpec?)
  ;

alterTblPartitionStatement
@init {msgs.push("alter table partition statement");}
@after {msgs.pop();}
  :  tablePartitionPrefix alterTblPartitionStatementSuffix
  -> ^(TOK_ALTERTABLE_PARTITION tablePartitionPrefix alterTblPartitionStatementSuffix)
  ;

alterTblPartitionStatementSuffix
@init {msgs.push("alter table partition statement suffix");}
@after {msgs.pop();}
  : alterStatementSuffixFileFormat
  | alterStatementSuffixLocation
  | alterStatementSuffixProtectMode
  | alterStatementSuffixMergeFiles
  | alterStatementSuffixSerdeProperties
  | alterStatementSuffixRenamePart
  | alterTblPartitionStatementSuffixSkewedLocation
    ;

alterStatementSuffixFileFormat
@init {msgs.push("alter fileformat statement"); }
@after {msgs.pop();}
	: KW_SET KW_FILEFORMAT fileFormat
	-> ^(TOK_ALTERTABLE_FILEFORMAT fileFormat)
	;

alterTblPartitionStatementSuffixSkewedLocation
@init {msgs.push("alter partition skewed location");}
@after {msgs.pop();}
  : KW_SET KW_SKEWED KW_LOCATION skewedLocations
  -> ^(TOK_ALTERTBLPART_SKEWED_LOCATION skewedLocations)
  ;
  
skewedLocations
@init { msgs.push("skewed locations"); }
@after { msgs.pop(); }
    :
      LPAREN skewedLocationsList RPAREN -> ^(TOK_SKEWED_LOCATIONS skewedLocationsList)
    ;

skewedLocationsList
@init { msgs.push("skewed locations list"); }
@after { msgs.pop(); }
    :
      skewedLocationMap (COMMA skewedLocationMap)* -> ^(TOK_SKEWED_LOCATION_LIST skewedLocationMap+)
    ;

skewedLocationMap
@init { msgs.push("specifying skewed location map"); }
@after { msgs.pop(); }
    :
      key=skewedValueLocationElement EQUAL value=StringLiteral -> ^(TOK_SKEWED_LOCATION_MAP $key $value)
    ;

alterStatementSuffixLocation
@init {msgs.push("alter location");}
@after {msgs.pop();}
  : KW_SET KW_LOCATION newLoc=StringLiteral
  -> ^(TOK_ALTERTABLE_LOCATION $newLoc)
  ;

	
alterStatementSuffixSkewedby
@init {msgs.push("alter skewed by statement");}
@after{msgs.pop();}
	:name=Identifier tableSkewed
	->^(TOK_ALTERTABLE_SKEWED $name tableSkewed)
	|
	name=Identifier KW_NOT KW_SKEWED
	->^(TOK_ALTERTABLE_SKEWED $name)
	|
	name=Identifier KW_NOT storedAsDirs
	->^(TOK_ALTERTABLE_SKEWED $name storedAsDirs)
	;

alterStatementSuffixProtectMode
@init { msgs.push("alter partition protect mode statement"); }
@after { msgs.pop(); }
    : alterProtectMode
    -> ^(TOK_ALTERTABLE_ALTERPARTS_PROTECTMODE alterProtectMode)
    ;

alterStatementSuffixRenamePart
@init { msgs.push("alter table rename partition statement"); }
@after { msgs.pop(); }
    : KW_RENAME KW_TO partitionSpec
    ->^(TOK_ALTERTABLE_RENAMEPART partitionSpec)
    ;

alterStatementSuffixMergeFiles
@init { msgs.push(""); }
@after { msgs.pop(); }
    : KW_CONCATENATE
    -> ^(TOK_ALTERTABLE_ALTERPARTS_MERGEFILES)
    ;

alterProtectMode
@init { msgs.push("protect mode specification enable"); }
@after { msgs.pop(); }
    : KW_ENABLE alterProtectModeMode  -> ^(TOK_ENABLE alterProtectModeMode)
    | KW_DISABLE alterProtectModeMode  -> ^(TOK_DISABLE alterProtectModeMode)
    ;

alterProtectModeMode
@init { msgs.push("protect mode specification enable"); }
@after { msgs.pop(); }
    : KW_OFFLINE  -> ^(TOK_OFFLINE)
    | KW_NO_DROP KW_CASCADE? -> ^(TOK_NO_DROP KW_CASCADE?)
    | KW_READONLY  -> ^(TOK_READONLY)
    ;


alterStatementSuffixClusterbySortby
@init {msgs.push("alter cluster by sort by statement");}
@after{msgs.pop();}
	:name=Identifier tableBuckets
	->^(TOK_ALTERTABLE_CLUSTER_SORT $name tableBuckets)
	|
	name=Identifier KW_NOT KW_CLUSTERED
	->^(TOK_ALTERTABLE_CLUSTER_SORT $name)
	;

fileFormat
@init { msgs.push("file format specification"); }
@after { msgs.pop(); }
    : KW_SEQUENCEFILE  -> ^(TOK_TBLSEQUENCEFILE)
    | KW_TEXTFILE  -> ^(TOK_TBLTEXTFILE)
    | KW_RCFILE  -> ^(TOK_TBLRCFILE)
    | KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt $inDriver? $outDriver?)
    | genericSpec=Identifier -> ^(TOK_FILEFORMAT_GENERIC $genericSpec)
    ;

tabTypeExpr
@init { msgs.push("specifying table types"); }
@after { msgs.pop(); }

   : Identifier (DOT^ (Identifier | KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE))*
   ;

descTabTypeExpr
@init { msgs.push("specifying describe table types"); }
@after { msgs.pop(); }

   : Identifier (DOT^ (Identifier | KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE))* Identifier?
   ;

partTypeExpr
@init { msgs.push("specifying table partitions"); }
@after { msgs.pop(); }
    :  tabTypeExpr partitionSpec? -> ^(TOK_TABTYPE tabTypeExpr partitionSpec?)
    ;

descPartTypeExpr
@init { msgs.push("specifying describe table partitions"); }
@after { msgs.pop(); }
    :  descTabTypeExpr partitionSpec? -> ^(TOK_TABTYPE descTabTypeExpr partitionSpec?)
    ;

descStatement
@init { msgs.push("describe statement"); }
@after { msgs.pop(); }
    : (KW_DESCRIBE|KW_DESC) (descOptions=KW_FORMATTED|descOptions=KW_EXTENDED)? (parttype=descPartTypeExpr) -> ^(TOK_DESCTABLE $parttype $descOptions?)
    | (KW_DESCRIBE|KW_DESC) KW_FUNCTION KW_EXTENDED? (name=descFuncNames) -> ^(TOK_DESCFUNCTION $name KW_EXTENDED?)
    | (KW_DESCRIBE|KW_DESC) KW_DATABASE KW_EXTENDED? (dbName=Identifier) -> ^(TOK_DESCDATABASE $dbName KW_EXTENDED?)
    | (KW_DESCRIBE|KW_DESC) KW_SCHEMA (schema=Identifier) -> ^(TOK_DESCSCHEMA $schema)
    ;

analyzeStatement
@init { msgs.push("analyze statement"); }
@after { msgs.pop(); }
    : KW_ANALYZE KW_TABLE (parttype=tableOrPartition) KW_COMPUTE KW_STATISTICS (KW_FOR KW_COLUMNS statsColumnName=columnNameList)? -> ^(TOK_ANALYZE $parttype $statsColumnName?)
    ;

showStatement
@init { msgs.push("show statement"); }
@after { msgs.pop(); }
    : KW_SHOW KW_DATABASES (dc_name=Identifier )?  (KW_LIKE  showStmtIdentifier)? -> ^(TOK_SHOWDATABASES $dc_name? (KW_LIKE showStmtIdentifier)?)
    | KW_SHOW KW_NODEGROUPS showStmtIdentifier?  -> ^(TOK_SHOWNODEGROUPS showStmtIdentifier?)
    | KW_SHOW KW_SCHEMAS showStmtIdentifier?  -> ^(TOK_SHOWSCHEMAS showStmtIdentifier?)
    | KW_SHOW KW_BUSITYPES  -> ^(TOK_SHOWBUSITYPES )
    | KW_SHOW KW_NODES (dc_name=Identifier )? -> ^(TOK_SHOWNODES $dc_name?)
    | KW_SHOW KW_FILES (part_name=StringLiteral (KW_FROM|KW_IN) tabname=tableName)? -> ^(TOK_SHOWFILES ($part_name $tabname)?)
    | KW_SHOW KW_FILELOCATIONS (part_name=StringLiteral (KW_FROM|KW_IN) tabname=tableName)? -> ^(TOK_SHOWFILELOCATIONS ($part_name $tabname)?)
    | KW_SHOW KW_TABLES ((KW_FROM|KW_IN) (dc_name=Identifier DOT)? db_name=Identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?  -> ^(TOK_SHOWTABLES (TOK_FROM ($dc_name TOK_FROM)? $db_name)? showStmtIdentifier?)
    | KW_SHOW KW_COLUMNS (KW_FROM|KW_IN) tabname=tableName ((KW_FROM|KW_IN) db_name=Identifier)? 
    -> ^(TOK_SHOWCOLUMNS $db_name? $tabname)
    | KW_SHOW KW_FUNCTIONS showStmtIdentifier?  -> ^(TOK_SHOWFUNCTIONS showStmtIdentifier?)
    | KW_SHOW KW_PARTITIONS Identifier partitionSpec? -> ^(TOK_SHOWPARTITIONS Identifier partitionSpec?)
    | KW_SHOW KW_PARTITION_KEYS (KW_FROM|KW_IN) tabname=tableName -> ^(TOK_SHOWPARTITIONKEYS $tabname)
    | KW_SHOW KW_SUBPARTITIONS part_name=StringLiteral KW_ON tabname=tableName -> ^(TOK_SHOWSUBPARTITIONS $part_name $tabname)
    | KW_SHOW KW_CREATE KW_TABLE tabName=tableName -> ^(TOK_SHOW_CREATETABLE $tabName)
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) db_name=Identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    -> ^(TOK_SHOW_TABLESTATUS showStmtIdentifier $db_name? partitionSpec?)
    | KW_SHOW KW_TBLPROPERTIES tblName=Identifier (LPAREN prptyName=StringLiteral RPAREN)? -> ^(TOK_SHOW_TBLPROPERTIES $tblName $prptyName?)
    | KW_SHOW KW_LOCKS (parttype=partTypeExpr)? (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWLOCKS $parttype? $isExtended?)
    | KW_SHOW (showOptions=KW_FORMATTED)? (KW_INDEX|KW_INDEXES) KW_ON showStmtIdentifier ((KW_FROM|KW_IN) db_name=Identifier)?
    -> ^(TOK_SHOWINDEXES showStmtIdentifier $showOptions? $db_name?)
    ;

lockStatement
@init { msgs.push("lock statement"); }
@after { msgs.pop(); }
    : KW_LOCK KW_TABLE tableName partitionSpec? lockMode -> ^(TOK_LOCKTABLE tableName lockMode partitionSpec?)
    ;

lockMode
@init { msgs.push("lock mode"); }
@after { msgs.pop(); }
    : KW_SHARED | KW_EXCLUSIVE
    ;

unlockStatement
@init { msgs.push("unlock statement"); }
@after { msgs.pop(); }
    : KW_UNLOCK KW_TABLE tableName partitionSpec?  -> ^(TOK_UNLOCKTABLE tableName partitionSpec?)
    ;

createRoleStatement
@init { msgs.push("create role"); }
@after { msgs.pop(); }
    : KW_CREATE kwRole roleName=Identifier
    -> ^(TOK_CREATEROLE $roleName)
    ;

dropRoleStatement
@init {msgs.push("drop role");}
@after {msgs.pop();}
    : KW_DROP kwRole roleName=Identifier
    -> ^(TOK_DROPROLE $roleName)
    ;

grantPrivileges
@init {msgs.push("grant privileges");}
@after {msgs.pop();}
    : KW_GRANT privList=privilegeList
      privilegeObject?
      KW_TO principalSpecification
      (KW_WITH withOption)?
    -> ^(TOK_GRANT $privList principalSpecification privilegeObject? withOption?)
    ;

revokePrivileges
@init {msgs.push("revoke privileges");}
@afer {msgs.pop();}
    : KW_REVOKE privilegeList privilegeObject? KW_FROM principalSpecification
    -> ^(TOK_REVOKE privilegeList principalSpecification privilegeObject?)
    ;

grantRole
@init {msgs.push("grant role");}
@after {msgs.pop();}
    : KW_GRANT kwRole Identifier (COMMA Identifier)* KW_TO principalSpecification
    -> ^(TOK_GRANT_ROLE principalSpecification Identifier+)
    ;

revokeRole
@init {msgs.push("revoke role");}
@after {msgs.pop();}
    : KW_REVOKE kwRole Identifier (COMMA Identifier)* KW_FROM principalSpecification
    -> ^(TOK_REVOKE_ROLE principalSpecification Identifier+)
    ;

//added by liulichao, ACL statements\

createUserStatement
@init { msgs.push("create user"); }
@after { msgs.pop(); }
//    : KW_CREATE kwUser userName=Identifier KW_IDENTIFIED KW_BY pwd=StringLiteral (asDBA)?
    : KW_CREATE kwUser userName=Identifier KW_IDENTIFIED KW_BY pwd=StringLiteral (asDBA)?
    -> ^(TOK_CREATEUSER $userName $pwd (asDBA)?)
    ;

dropUserStatement
@init {msgs.push("drop user");}
@after {msgs.pop();}
    : KW_DROP kwUser Identifier (COMMA Identifier)*
//    : KW_DROP kwUser userName=Identifier
//    -> ^(TOK_DROPUSER $userName)
//    : KW_DROP kwUser  Identifier
    -> ^(TOK_DROPUSER Identifier+)
    ;

showUserNames
@init {msgs.push("show user names");}
@after {msgs.pop();}
    : KW_SHOW kwUsers
    -> ^(TOK_SHOW_USERNAMES)
    ;

changePassword
@init {msgs.push("change user's password");}
@after {msgs.pop();}
    : KW_SETPASSWD (KW_FOR user=Identifier)? KW_TO pwd=StringLiteral
    -> ^(TOK_CHANGE_PWD $user? $pwd)	//TOK_CHANGE_PWD StringLiteral $user?
    ;

/*
switchUserStatement
@init { msgs.push("switch user statement"); }
@after { msgs.pop(); }
    : KW_USE kwUser Identifier
    -> ^(TOK_SWITCHUSER Identifier)
    ;
    */
authentificationStatement
@init { msgs.push("authentification statement"); }
@after { msgs.pop(); }
    : KW_CONNECT userName=Identifier  passord=StringLiteral
    -> ^(TOK_AUTHENTICATION $userName $passord)
    ;

//added by liulichao

showRoleGrants
@init {msgs.push("show role grants");}
@after {msgs.pop();}
    : KW_SHOW kwRole KW_GRANT principalName
    -> ^(TOK_SHOW_ROLE_GRANT principalName)
    ;

showGrants
@init {msgs.push("show grants");}
@after {msgs.pop();}
    : KW_SHOW KW_GRANT principalName privilegeIncludeColObject?
    -> ^(TOK_SHOW_GRANT principalName privilegeIncludeColObject?)
    ;

privilegeIncludeColObject
@init {msgs.push("privilege object including columns");}
@after {msgs.pop();}
    : KW_ON (table=KW_TABLE|KW_DATABASE) Identifier (LPAREN cols=columnNameList RPAREN)? partitionSpec?
    -> ^(TOK_PRIV_OBJECT_COL Identifier $table? $cols? partitionSpec?)
    ;

privilegeObject
@init {msgs.push("privilege subject");}
@after {msgs.pop();}
    : KW_ON (table=KW_TABLE|KW_DATABASE) Identifier partitionSpec?
    -> ^(TOK_PRIV_OBJECT Identifier $table? partitionSpec?)
    ;

privilegeList
@init {msgs.push("grant privilege list");}
@after {msgs.pop();}
    : privlegeDef (COMMA privlegeDef)*
    -> ^(TOK_PRIVILEGE_LIST privlegeDef+)
    ;

privlegeDef
@init {msgs.push("grant privilege");}
@after {msgs.pop();}
    : privilegeType (LPAREN cols=columnNameList RPAREN)?
    -> ^(TOK_PRIVILEGE privilegeType $cols?)
    ;

privilegeType
@init {msgs.push("privilege type");}
@after {msgs.pop();}
    : KW_ALL -> ^(TOK_PRIV_ALL)
    | KW_ALTER -> ^(TOK_PRIV_ALTER_METADATA)
    | KW_UPDATE -> ^(TOK_PRIV_ALTER_DATA)
    | KW_CREATE -> ^(TOK_PRIV_CREATE)
    | KW_DROP -> ^(TOK_PRIV_DROP)
    | KW_INDEX -> ^(TOK_PRIV_INDEX)
    | KW_LOCK -> ^(TOK_PRIV_LOCK)
    | KW_SELECT -> ^(TOK_PRIV_SELECT)
    | KW_SHOW_DATABASE -> ^(TOK_PRIV_SHOW_DATABASE)
    | KW_DBA->^(TOK_PRIV_DBA)	//added by liulichao
    ;

principalSpecification
@init { msgs.push("user/group/role name list"); }
@after { msgs.pop(); }
    : principalName (COMMA principalName)* -> ^(TOK_PRINCIPAL_NAME principalName+)
    ;

principalName
@init {msgs.push("user|group|role name");}
@after {msgs.pop();}
    : kwUser Identifier -> ^(TOK_USER Identifier)
    | KW_GROUP Identifier -> ^(TOK_GROUP Identifier)
    | kwRole Identifier -> ^(TOK_ROLE Identifier)
    ;

withOption
@init {msgs.push("grant with option");}
@after {msgs.pop();}
    : KW_GRANT KW_OPTION
    -> ^(TOK_GRANT_WITH_OPTION)
    ;

//added by liulichao
asDBA
@init {msgs.push("as DBA");}
@after {msgs.pop();}
    : KW_AS KW_DBA
    -> ^(TOK_CREATE_AS_DBA)
    ;

metastoreCheck
@init { msgs.push("metastore check statement"); }
@after { msgs.pop(); }
    : KW_MSCK (repair=KW_REPAIR)? (KW_TABLE table=Identifier partitionSpec? (COMMA partitionSpec)*)?
    -> ^(TOK_MSCK $repair? ($table partitionSpec*)?)
    ;

createFunctionStatement
@init { msgs.push("create function statement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_TEMPORARY KW_FUNCTION Identifier KW_AS StringLiteral
    -> ^(TOK_CREATEFUNCTION Identifier StringLiteral)
    ;

dropFunctionStatement
@init { msgs.push("drop temporary function statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_TEMPORARY KW_FUNCTION ifExists? Identifier
    -> ^(TOK_DROPFUNCTION Identifier ifExists?)
    ;
    
heterOption
@init { msgs.push("drop temporary function statement"); }
@after { msgs.pop(); }
	:KW_HETER
	-> ^(TOK_HETER)
	;

createViewStatement
@init {
    msgs.push("create view statement");
}
@after { msgs.pop(); }
    : KW_CREATE (orReplace)? heterOption? KW_VIEW (ifNotExists)? name=tableName
        (LPAREN columnNameCommentList RPAREN)? tableComment? viewPartition?
        tablePropertiesPrefixed?
        KW_AS
        selectStatement
    -> ^(TOK_CREATEVIEW $name heterOption? orReplace?
         ifNotExists?
         columnNameCommentList?
         tableComment?
         viewPartition?
         tablePropertiesPrefixed?
         selectStatement
        )
    ;

viewPartition
@init { msgs.push("view partition specification"); }
@after { msgs.pop(); }
    : KW_PARTITIONED KW_ON LPAREN columnNameList RPAREN
    -> ^(TOK_VIEWPARTCOLS columnNameList)
    ;

dropViewStatement
@init { msgs.push("drop view statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_VIEW ifExists? viewName -> ^(TOK_DROPVIEW viewName ifExists?)
    ;

showStmtIdentifier
@init { msgs.push("Identifier for show statement"); }
@after { msgs.pop(); }
    : Identifier
    | StringLiteral
    ;

tableComment
@init { msgs.push("table's comment"); }
@after { msgs.pop(); }
    :
      KW_COMMENT comment=StringLiteral  -> ^(TOK_TABLECOMMENT $comment)
    ;

fileSplit
@init { msgs.push("file split specification"); }
@after { msgs.pop(); }
    : KW_SPLITED KW_BY splitParamList=partitionParamList
    fileSubSplit?
    splitTemplate?
    -> ^(TOK_SPLITED_BY $splitParamList
    fileSubSplit?
    splitTemplate?)
    ;
    
    
tablePartition
@init { msgs.push("table partition specification"); }
@after { msgs.pop(); }
    : KW_PARTITIONED KW_BY partParamList=partitionParamList
    tableSubPartition?
    partitionTemplate?
    -> ^(TOK_PARTITIONED_BY $partParamList
    tableSubPartition?
    partitionTemplate?)
    ;
    
fileSubSplit
@init { msgs.push("file subSplit specification"); }
@after { msgs.pop(); }
    :KW_SUBSPLITED KW_BY splitParamList=partitionParamList
   subSplitTemplate?
    -> ^(TOK_SUBSPLITED_BY $splitParamList
    subSplitTemplate?)
    ;
    
tableSubPartition
@init { msgs.push("table subPartition specification"); }
@after { msgs.pop(); }
    :KW_SUBPARTITIONED KW_BY partParamList=partitionParamList
   subPartitionTemplate?
    -> ^(TOK_SUBPARTITIONED_BY $partParamList
    subPartitionTemplate?)
    ;
    
partitionParamList
@init { msgs.push(" partitionParamList specification"); }
@after { msgs.pop(); }
    :(LPAREN (columnNameTypeList
    	|columnNameList
   	| functionList
    ) RPAREN)  
    | (columnNameTypeList
    	|columnNameList
   	| functionList)
    ;
  
  splitTemplate
@init { msgs.push("file SpiltTemplate specification"); }
@after { msgs.pop(); }
    :LPAREN splitExper (COMMA splitExper)* RPAREN
     -> ^(TOK_SPLIT_EXPER splitExper+)
    ;
      
partitionTemplate
@init { msgs.push("table PartitionTemplate specification"); }
@after { msgs.pop(); }
    :LPAREN partitionExper (COMMA partitionExper)* RPAREN
     -> ^(TOK_PARTITION_EXPER partitionExper+)
    ;
    
 subSplitTemplate
@init { msgs.push(" splitTemplate specification"); }
@after { msgs.pop(); }
    :LPAREN  (subSplitExper (COMMA subSplitExper)*) RPAREN
    -> ^(TOK_SUBSPLIT_EXPER subSplitExper+)
    ;
    
subPartitionTemplate
@init { msgs.push(" subPartitionTemplate specification"); }
@after { msgs.pop(); }
    :LPAREN  (subPartitionExper (COMMA subPartitionExper)*) RPAREN
    -> ^(TOK_SUBPARTITION_EXPER subPartitionExper+)
    ;

splitExper
@init { msgs.push("splitExper specification"); }
@after { msgs.pop(); }
    :KW_SPLIT split_name=splitName  partitionValuesExper  subSplitTemplate?
    -> ^(TOK_SPLIT $split_name partitionValuesExper subSplitTemplate?)
    ;
    
partitionExper
@init { msgs.push("partitionExper specification"); }
@after { msgs.pop(); }
    :KW_PARTITION partition_name=partitionName  partitionValuesExper  subSplitTemplate?
    -> ^(TOK_PARTITION $partition_name partitionValuesExper subSplitTemplate?)
    ;
    
partitionValuesExper
@init { msgs.push(" partitionValuesExper specification"); }
@after { msgs.pop(); }
    :KW_VALUES  KW_LESS KW_THAN LPAREN value=stringOrNumOrFunc RPAREN 
    -> ^(TOK_VALUES_LESS  $value )
    |KW_VALUES  KW_GREATER KW_THAN LPAREN value=stringOrNumOrFunc RPAREN 
     -> ^(TOK_VALUES_GREATER  $value )
    | KW_VALUES LPAREN valueList=stringOrNumOrFuncList RPAREN
     -> ^(TOK_VALUES   $valueList )
    ;
    
splitValuesExper
@init { msgs.push(" splitValuesExper specification"); }
@after { msgs.pop(); }
    :KW_VALUES  KW_LESS KW_THAN LPAREN value=stringOrNumOrFunc RPAREN 
    -> ^(TOK_VALUES_LESS  $value )
    |KW_VALUES  KW_GREATER KW_THAN LPAREN value=stringOrNumOrFunc RPAREN 
     -> ^(TOK_VALUES_GREATER  $value )
    | KW_VALUES LPAREN valueList=stringOrNumOrFuncList RPAREN
     -> ^(TOK_VALUES   $valueList )
    ;
       
subSplitExper
@init { msgs.push("subSplitExper specification"); }
@after { msgs.pop(); }
      :KW_SUBSPLIT split_name=splitName  partitionValuesExper 
    -> ^(TOK_SUBSPLIT $split_name partitionValuesExper)
    ;

subPartitionExper
@init { msgs.push("subPartitionExper specification"); }
@after { msgs.pop(); }
/*
    :KW_SUBPARTITION partitionName (
     (KW_VALUES ((KW_LESS|KW_GREATER) KW_THEN) LPAREN (stringOrNumOrFunc) RPAREN)
     | KW_VALUES LPAREN (stringOrNumOrFuncList) RPAREN)
    ;
    */
      :KW_SUBPARTITION partition_name=partitionName  partitionValuesExper 
    -> ^(TOK_SUBPARTITION $partition_name partitionValuesExper)
    ;
    
stringOrNumOrFunc
 @init { msgs.push("table stringOrNumOrFunc specification"); }
@after { msgs.pop(); }
    :StringLiteral
    |Number
    |function
    ;
    
 stringOrNumOrFuncList  
@init { msgs.push("table stringOrNumOrFuncList specification"); }
@after { msgs.pop(); }
 :(stringOrNumOrFunc (COMMA stringOrNumOrFunc)*)
 -> ^(TOK_STR_OR_NUM_OR_FUNC stringOrNumOrFunc+)
    ;
 
 splitName
@init { msgs.push("file splitName specification"); }
@after { msgs.pop(); }
    :
      Identifier
    ;
         
partitionName
@init { msgs.push("table partitionName specification"); }
@after { msgs.pop(); }
    :
      Identifier
    ;
    
 datawarehouseStatement
@init { msgs.push("table datawarehouseStatement specification"); }
@after { msgs.pop(); }
    :
      KW_ALTER KW_DW  KW_DIRECT LPAREN dwNo=Number COMMA sql=StringLiteral RPAREN
       -> ^(TOK_ALTER_DW $dwNo $sql)
    ;

//added by zjw to customize table partition
functionList
@init { msgs.push("table partition function specification"); }
@after { msgs.pop(); }
    :
    function  (COMMA function)*  -> ^(TOK_TABLEPARTCOLS (function)+)
    ;	

tableBuckets
@init { msgs.push("table buckets specification"); }
@after { msgs.pop(); }
    :
      KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS
    -> ^(TOK_TABLEBUCKETS $bucketCols $sortCols? $num)
    ;

tableSkewed
@init { msgs.push("table skewed specification"); }
@after { msgs.pop(); }
    :
     KW_SKEWED KW_BY LPAREN skewedCols=columnNameList RPAREN KW_ON LPAREN (skewedValues=skewedValueElement) RPAREN (storedAsDirs)?
    -> ^(TOK_TABLESKEWED $skewedCols $skewedValues storedAsDirs?)
    ;

rowFormat
@init { msgs.push("serde specification"); }
@after { msgs.pop(); }
    : rowFormatSerde -> ^(TOK_SERDE rowFormatSerde)
    | rowFormatDelimited -> ^(TOK_SERDE rowFormatDelimited)
    |   -> ^(TOK_SERDE)
    ;

recordReader
@init { msgs.push("record reader specification"); }
@after { msgs.pop(); }
    : KW_RECORDREADER StringLiteral -> ^(TOK_RECORDREADER StringLiteral)
    |   -> ^(TOK_RECORDREADER)
    ;

recordWriter
@init { msgs.push("record writer specification"); }
@after { msgs.pop(); }
    : KW_RECORDWRITER StringLiteral -> ^(TOK_RECORDWRITER StringLiteral)
    |   -> ^(TOK_RECORDWRITER)
    ;

rowFormatSerde
@init { msgs.push("serde format specification"); }
@after { msgs.pop(); }
    : KW_ROW KW_FORMAT KW_SERDE name=StringLiteral (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
    -> ^(TOK_SERDENAME $name $serdeprops?)
    ;

rowFormatDelimited
@init { msgs.push("serde properties specification"); }
@after { msgs.pop(); }
    :
      KW_ROW KW_FORMAT KW_DELIMITED tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier?
    -> ^(TOK_SERDEPROPS tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier?)
    ;

tableRowFormat
@init { msgs.push("table row format specification"); }
@after { msgs.pop(); }
    :
      rowFormatDelimited
    -> ^(TOK_TABLEROWFORMAT rowFormatDelimited)
    | rowFormatSerde
    -> ^(TOK_TABLESERIALIZER rowFormatSerde)
    ;

tablePropertiesPrefixed
@init { msgs.push("table properties with prefix"); }
@after { msgs.pop(); }
    :
        KW_TBLPROPERTIES! tableProperties
    ;

 schemaPropertiesPrefixed
@init { msgs.push("schema properties with prefix"); }
@after { msgs.pop(); }
    :
        KW_SCHEMAPROPERTIES! tableProperties
    ;

tableProperties
@init { msgs.push("table properties"); }
@after { msgs.pop(); }
    :
      LPAREN tablePropertiesList RPAREN -> ^(TOK_TABLEPROPERTIES tablePropertiesList)
    ;

tablePropertiesList
@init { msgs.push("table properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_TABLEPROPLIST keyValueProperty+)
    ;

keyValueProperty
@init { msgs.push("specifying key/value property"); }
@after { msgs.pop(); }
    :
      key=StringLiteral EQUAL value=StringLiteral -> ^(TOK_TABLEPROPERTY $key $value)
    ;

tableRowFormatFieldIdentifier
@init { msgs.push("table row format's field separator"); }
@after { msgs.pop(); }
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=StringLiteral (KW_ESCAPED KW_BY fldEscape=StringLiteral)?
    -> ^(TOK_TABLEROWFORMATFIELD $fldIdnt $fldEscape?)
    ;

tableRowFormatCollItemsIdentifier
@init { msgs.push("table row format's column separator"); }
@after { msgs.pop(); }
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATCOLLITEMS $collIdnt)
    ;

tableRowFormatMapKeysIdentifier
@init { msgs.push("table row format's map key separator"); }
@after { msgs.pop(); }
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATMAPKEYS $mapKeysIdnt)
    ;

tableRowFormatLinesIdentifier
@init { msgs.push("table row format's line separator"); }
@after { msgs.pop(); }
    :
      KW_LINES KW_TERMINATED KW_BY linesIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATLINES $linesIdnt)
    ;

tableFileFormat
@init { msgs.push("table file format specification"); }
@after { msgs.pop(); }
    :
      KW_STORED KW_AS KW_SEQUENCEFILE  -> TOK_TBLSEQUENCEFILE
      | KW_STORED KW_AS KW_TEXTFILE  -> TOK_TBLTEXTFILE
      | KW_STORED KW_AS KW_RCFILE  -> TOK_TBLRCFILE
      | KW_STORED KW_AS KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt $inDriver? $outDriver?)
      | KW_STORED KW_BY storageHandler=StringLiteral
         (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
      -> ^(TOK_STORAGEHANDLER $storageHandler $serdeprops?)
      | KW_STORED KW_AS genericSpec=Identifier
      -> ^(TOK_FILEFORMAT_GENERIC $genericSpec)
    ;

tableDistribution
@init { msgs.push("table location specification"); }
@after { msgs.pop(); }
    :
      KW_DISTRIBUTE KW_ON KW_NODEGROUP nodegroupnames=stringLiteralList-> ^(TOK_TABLEDISTRIBUTION $nodegroupnames)
    ;

tableLocation
@init { msgs.push("table location specification"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_TABLELOCATION $locn)
    ;

columnNameTypeList
@init { msgs.push("column name type list"); }
@after { msgs.pop(); }
    : columnNameType (COMMA columnNameType)* -> ^(TOK_TABCOLLIST columnNameType+)
    ;

columnNameColonTypeList
@init { msgs.push("column name type list"); }
@after { msgs.pop(); }
    : columnNameColonType (COMMA columnNameColonType)* -> ^(TOK_TABCOLLIST columnNameColonType+)
    ;

columnNameList
@init { msgs.push("column name list"); }
@after { msgs.pop(); }
    : columnName (COMMA columnName)* -> ^(TOK_TABCOLNAME columnName+)
    ;

columnName
@init { msgs.push("column name"); }
@after { msgs.pop(); }
    :
      Identifier
    ;

columnNameOrderList
@init { msgs.push("column name order list"); }
@after { msgs.pop(); }
    : columnNameOrder (COMMA columnNameOrder)* -> ^(TOK_TABCOLNAME columnNameOrder+)
    ;

skewedValueElement
@init { msgs.push("skewed value element"); }
@after { msgs.pop(); }
    : 
      skewedColumnValues
     | skewedColumnValuePairList
    ;

skewedColumnValuePairList
@init { msgs.push("column value pair list"); }
@after { msgs.pop(); }
    : skewedColumnValuePair (COMMA skewedColumnValuePair)* -> ^(TOK_TABCOLVALUE_PAIR skewedColumnValuePair+)
    ;

skewedColumnValuePair
@init { msgs.push("column value pair"); }
@after { msgs.pop(); }
    : 
      LPAREN colValues=skewedColumnValues RPAREN 
      -> ^(TOK_TABCOLVALUES $colValues)
    ;

skewedColumnValues
@init { msgs.push("column values"); }
@after { msgs.pop(); }
    : skewedColumnValue (COMMA skewedColumnValue)* -> ^(TOK_TABCOLVALUE skewedColumnValue+)
    ;

skewedColumnValue
@init { msgs.push("column value"); }
@after { msgs.pop(); }
    :
      constant
    ;

skewedValueLocationElement
@init { msgs.push("skewed value location element"); }
@after { msgs.pop(); }
    : 
      skewedColumnValue
     | skewedColumnValuePair
    ;
    
columnNameOrder
@init { msgs.push("column name order"); }
@after { msgs.pop(); }
    : Identifier (asc=KW_ASC | desc=KW_DESC)?
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC Identifier)
    ->                  ^(TOK_TABSORTCOLNAMEDESC Identifier)
    ;

columnNameCommentList
@init { msgs.push("column name comment list"); }
@after { msgs.pop(); }
    : columnNameComment (COMMA columnNameComment)* -> ^(TOK_TABCOLNAME columnNameComment+)
    ;

columnNameComment
@init { msgs.push("column name comment"); }
@after { msgs.pop(); }
    : colName=Identifier (KW_COMMENT comment=StringLiteral)?
    -> ^(TOK_TABCOL $colName TOK_NULL $comment?)
    ;

columnRefOrder
@init { msgs.push("column order"); }
@after { msgs.pop(); }
    : expression (asc=KW_ASC | desc=KW_DESC)?
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC expression)
    ->                  ^(TOK_TABSORTCOLNAMEDESC expression)
    ;

columnNameType
@init { msgs.push("column specification"); }
@after { msgs.pop(); }
    : colName=Identifier colType (KW_COMMENT comment=StringLiteral)?
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

columnNameColonType
@init { msgs.push("column specification"); }
@after { msgs.pop(); }
    : colName=Identifier COLON colType (KW_COMMENT comment=StringLiteral)?
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

colType
@init { msgs.push("column type"); }
@after { msgs.pop(); }
    : type
    ;

colTypeList
@init { msgs.push("column type list"); }
@after { msgs.pop(); }
    : colType (COMMA colType)* -> ^(TOK_COLTYPELIST colType+)
    ;

type
    : primitiveType
    | listType
    | structType
    | mapType
    | unionType;

primitiveType
@init { msgs.push("primitive type specification"); }
@after { msgs.pop(); }
    : KW_TINYINT       ->    TOK_TINYINT
    | KW_SMALLINT      ->    TOK_SMALLINT
    | KW_INT           ->    TOK_INT
    | KW_BIGINT        ->    TOK_BIGINT
    | KW_BOOLEAN       ->    TOK_BOOLEAN
    | KW_FLOAT         ->    TOK_FLOAT
    | KW_DOUBLE        ->    TOK_DOUBLE
    | KW_DATE          ->    TOK_DATE
    | KW_DATETIME      ->    TOK_DATETIME
    | KW_TIMESTAMP     ->    TOK_TIMESTAMP
    | KW_STRING        ->    TOK_STRING
    | KW_BINARY        ->    TOK_BINARY
    | KW_BLOB        ->    TOK_BLOB    
    ;

listType
@init { msgs.push("list type"); }
@after { msgs.pop(); }
    : KW_ARRAY LESSTHAN type GREATERTHAN   -> ^(TOK_LIST type)
    ;

structType
@init { msgs.push("struct type"); }
@after { msgs.pop(); }
    : KW_STRUCT LESSTHAN columnNameColonTypeList GREATERTHAN -> ^(TOK_STRUCT columnNameColonTypeList)
    ;

mapType
@init { msgs.push("map type"); }
@after { msgs.pop(); }
    : KW_MAP LESSTHAN left=primitiveType COMMA right=type GREATERTHAN
    -> ^(TOK_MAP $left $right)
    ;

unionType
@init { msgs.push("uniontype type"); }
@after { msgs.pop(); }
    : KW_UNIONTYPE LESSTHAN colTypeList GREATERTHAN -> ^(TOK_UNIONTYPE colTypeList)
    ;

queryOperator
@init { msgs.push("query operator"); }
@after { msgs.pop(); }
    : KW_UNION KW_ALL -> ^(TOK_UNION)
    ;

// select statement select ... from ... where ... group by ... order by ...
queryStatementExpression
    : queryStatement (queryOperator^ queryStatement)*
    ;

queryStatement
    :
    fromClause
    ( b+=body )+ -> ^(TOK_QUERY fromClause body+)
    | regular_body
    ;

regular_body
   :
   insertClause
   selectClause
   fromClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_QUERY fromClause ^(TOK_INSERT insertClause
                     selectClause whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?))
   |
   selectStatement
   ;

selectStatement
   :
   selectClause
   fromClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_QUERY fromClause ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?))
   ;


body
   :
   insertClause
   selectClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_INSERT insertClause?
                     selectClause whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?)
   |
   selectClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?)
   ;

insertClause
@init { msgs.push("insert clause"); }
@after { msgs.pop(); }
   :
     KW_INSERT KW_OVERWRITE destination ifNotExists? -> ^(TOK_DESTINATION destination ifNotExists?)
   | KW_INSERT KW_INTO KW_TABLE tableOrPartition
       -> ^(TOK_INSERT_INTO ^(tableOrPartition))
   ;

destination
@init { msgs.push("destination specification"); }
@after { msgs.pop(); }
   :
     KW_LOCAL KW_DIRECTORY StringLiteral -> ^(TOK_LOCAL_DIR StringLiteral)
   | KW_DIRECTORY StringLiteral -> ^(TOK_DIR StringLiteral)
   | KW_TABLE tableOrPartition -> ^(tableOrPartition)
   ;

limitClause
@init { msgs.push("limit clause"); }
@after { msgs.pop(); }
   :
   KW_LIMIT num=Number -> ^(TOK_LIMIT $num)
   ;

//----------------------- Rules for parsing selectClause -----------------------------
// select a,b,c ...
selectClause
@init { msgs.push("select clause"); }
@after { msgs.pop(); }
    :
    KW_SELECT hintClause? (((KW_ALL | dist=KW_DISTINCT)? selectList)
                          | (transform=KW_TRANSFORM selectTrfmClause))
     -> {$transform == null && $dist == null}? ^(TOK_SELECT hintClause? selectList)
     -> {$transform == null && $dist != null}? ^(TOK_SELECTDI hintClause? selectList)
     -> ^(TOK_SELECT hintClause? ^(TOK_SELEXPR selectTrfmClause) )
    |
    trfmClause  ->^(TOK_SELECT ^(TOK_SELEXPR trfmClause))
    ;

selectList
@init { msgs.push("select list"); }
@after { msgs.pop(); }
    :
    selectItem ( COMMA  selectItem )* -> selectItem+
    ;

selectTrfmClause
@init { msgs.push("transform clause"); }
@after { msgs.pop(); }
    :
    LPAREN selectExpressionList RPAREN
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde $inRec StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;

hintClause
@init { msgs.push("hint clause"); }
@after { msgs.pop(); }
    :
    DIVIDE STAR PLUS hintList STAR DIVIDE -> ^(TOK_HINTLIST hintList)
    ;

hintList
@init { msgs.push("hint list"); }
@after { msgs.pop(); }
    :
    hintItem (COMMA hintItem)* -> hintItem+
    ;

hintItem
@init { msgs.push("hint item"); }
@after { msgs.pop(); }
    :
    hintName (LPAREN hintArgs RPAREN)? -> ^(TOK_HINT hintName hintArgs?)
    ;

hintName
@init { msgs.push("hint name"); }
@after { msgs.pop(); }
    :
    KW_MAPJOIN -> TOK_MAPJOIN
    | KW_STREAMTABLE -> TOK_STREAMTABLE
    | KW_HOLD_DDLTIME -> TOK_HOLD_DDLTIME
    ;

hintArgs
@init { msgs.push("hint arguments"); }
@after { msgs.pop(); }
    :
    hintArgName (COMMA hintArgName)* -> ^(TOK_HINTARGLIST hintArgName+)
    ;

hintArgName
@init { msgs.push("hint argument name"); }
@after { msgs.pop(); }
    :
    Identifier
    ;

selectItem
@init { msgs.push("selection target"); }
@after { msgs.pop(); }
    :
    ( selectExpression  ((KW_AS? Identifier) | (KW_AS LPAREN Identifier (COMMA Identifier)* RPAREN))?) -> ^(TOK_SELEXPR selectExpression Identifier*)
    ;

trfmClause
@init { msgs.push("transform clause"); }
@after { msgs.pop(); }
    :
    (   KW_MAP    selectExpressionList
      | KW_REDUCE selectExpressionList )
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde $inRec StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;

selectExpression
@init { msgs.push("select expression"); }
@after { msgs.pop(); }
    :
    expression | tableAllColumns
    ;

selectExpressionList
@init { msgs.push("select expression list"); }
@after { msgs.pop(); }
    :
    selectExpression (COMMA selectExpression)* -> ^(TOK_EXPLIST selectExpression+)
    ;


//-----------------------------------------------------------------------------------

tableAllColumns
    : STAR
        -> ^(TOK_ALLCOLREF)
    | tableName DOT STAR
        -> ^(TOK_ALLCOLREF tableName)
    ;

// (table|column)
tableOrColumn
@init { msgs.push("table or column identifier"); }
@after { msgs.pop(); }
    :
    Identifier -> ^(TOK_TABLE_OR_COL Identifier)
    ;

expressionList
@init { msgs.push("expression list"); }
@after { msgs.pop(); }
    :
    expression (COMMA expression)* -> ^(TOK_EXPLIST expression+)
    ;

aliasList
@init { msgs.push("alias list"); }
@after { msgs.pop(); }
    :
    Identifier (COMMA Identifier)* -> ^(TOK_ALIASLIST Identifier+)
    ;

//----------------------- Rules for parsing fromClause ------------------------------
// from [col1, col2, col3] table1, [col4, col5] table2
fromClause
@init { msgs.push("from clause"); }
@after { msgs.pop(); }
    :
    KW_FROM joinSource -> ^(TOK_FROM joinSource)
    ;

joinSource
@init { msgs.push("join source"); }
@after { msgs.pop(); }
    : fromSource ( joinToken^ fromSource (KW_ON! expression)? )*
    | uniqueJoinToken^ uniqueJoinSource (COMMA! uniqueJoinSource)+
    ;

uniqueJoinSource
@init { msgs.push("join source"); }
@after { msgs.pop(); }
    : KW_PRESERVE? fromSource uniqueJoinExpr
    ;

uniqueJoinExpr
@init { msgs.push("unique join expression list"); }
@after { msgs.pop(); }
    : LPAREN e1+=expression (COMMA e1+=expression)* RPAREN
      -> ^(TOK_EXPLIST $e1*)
    ;

uniqueJoinToken
@init { msgs.push("unique join"); }
@after { msgs.pop(); }
    : KW_UNIQUEJOIN -> TOK_UNIQUEJOIN;

joinToken
@init { msgs.push("join type specifier"); }
@after { msgs.pop(); }
    :
      KW_JOIN                     -> TOK_JOIN
    | kwInner  KW_JOIN            -> TOK_JOIN
    | KW_CROSS KW_JOIN            -> TOK_CROSSJOIN
    | KW_LEFT  KW_OUTER KW_JOIN   -> TOK_LEFTOUTERJOIN
    | KW_RIGHT KW_OUTER KW_JOIN   -> TOK_RIGHTOUTERJOIN
    | KW_FULL  KW_OUTER KW_JOIN   -> TOK_FULLOUTERJOIN
    | KW_LEFT  KW_SEMI  KW_JOIN   -> TOK_LEFTSEMIJOIN
    ;

lateralView
@init {msgs.push("lateral view"); }
@after {msgs.pop(); }
	:
	KW_LATERAL KW_VIEW function tableAlias KW_AS Identifier (COMMA Identifier)* -> ^(TOK_LATERAL_VIEW ^(TOK_SELECT ^(TOK_SELEXPR function Identifier+ tableAlias)))
	;

tableAlias
@init {msgs.push("table alias"); }
@after {msgs.pop(); }
    :
    Identifier -> ^(TOK_TABALIAS Identifier)
    ;

fromSource
@init { msgs.push("from source"); }
@after { msgs.pop(); }
    :
    (tableSource | subQuerySource) (lateralView^)*
    ;

tableBucketSample
@init { msgs.push("table bucket sample specification"); }
@after { msgs.pop(); }
    :
    KW_TABLESAMPLE LPAREN KW_BUCKET (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_ON expr+=expression (COMMA expr+=expression)*)? RPAREN -> ^(TOK_TABLEBUCKETSAMPLE $numerator $denominator $expr*)
    ;

splitSample
@init { msgs.push("table split sample specification"); }
@after { msgs.pop(); }
    :
    KW_TABLESAMPLE LPAREN  (numerator=Number) KW_PERCENT RPAREN -> ^(TOK_TABLESPLITSAMPLE $numerator)
    ;

tableSample
@init { msgs.push("table sample specification"); }
@after { msgs.pop(); }
    :
    tableBucketSample |
    splitSample
    ;

tableSource
@init { msgs.push("table source"); }
@after { msgs.pop(); }
    : tabname=tableName (ts=tableSample)? (alias=Identifier)?
    -> ^(TOK_TABREF $tabname $ts? $alias?)
    ;

tableName
@init { msgs.push("table name"); }
@after { msgs.pop(); }
    :(dc=Identifier DOT)? (db=Identifier DOT)? tab=Identifier
    -> ^(TOK_TABNAME $dc? $db? $tab)
    ;
    
schemaName
@init { msgs.push("schema name"); }
@after { msgs.pop(); }
    :schema=Identifier
    -> ^(TOK_SCHEMANAME $schema)
    ;

viewName
@init { msgs.push("view name"); }
@after { msgs.pop(); }
    :
    (db=Identifier DOT)? view=Identifier
    -> ^(TOK_TABNAME $db? $view)
    ;

subQuerySource
@init { msgs.push("subquery source"); }
@after { msgs.pop(); }
    :
    LPAREN queryStatementExpression RPAREN Identifier -> ^(TOK_SUBQUERY queryStatementExpression Identifier)
    ;

//----------------------- Rules for parsing whereClause -----------------------------
// where a=b and ...
whereClause
@init { msgs.push("where clause"); }
@after { msgs.pop(); }
    :
    KW_WHERE searchCondition -> ^(TOK_WHERE searchCondition)
    ;

searchCondition
@init { msgs.push("search condition"); }
@after { msgs.pop(); }
    :
    expression
    ;

//-----------------------------------------------------------------------------------

// group by a,b
groupByClause
@init { msgs.push("group by clause"); }
@after { msgs.pop(); }
    :
    KW_GROUP KW_BY
    groupByExpression
    ( COMMA groupByExpression )*
    ((rollup=KW_WITH KW_ROLLUP) | (cube=KW_WITH KW_CUBE)) ?
    (sets=KW_GROUPING KW_SETS 
    LPAREN groupingSetExpression ( COMMA groupingSetExpression)*  RPAREN ) ?
    -> {rollup != null}? ^(TOK_ROLLUP_GROUPBY groupByExpression+)
    -> {cube != null}? ^(TOK_CUBE_GROUPBY groupByExpression+)
    -> {sets != null}? ^(TOK_GROUPING_SETS groupByExpression+ groupingSetExpression+)
    -> ^(TOK_GROUPBY groupByExpression+)
    ;

groupingSetExpression
@init {msgs.push("grouping set expression"); }
@after {msgs.pop(); }
   :
   groupByExpression
   -> ^(TOK_GROUPING_SETS_EXPRESSION groupByExpression)
   |
   LPAREN 
   groupByExpression (COMMA groupByExpression)*
   RPAREN
   -> ^(TOK_GROUPING_SETS_EXPRESSION groupByExpression+)
   |
   LPAREN
   RPAREN
   //-> ^(TOK_GROUPING_SETS_EXPRESSION groupByExpression*)
   -> ^(TOK_GROUPING_SETS_EXPRESSION )
   ;


groupByExpression
@init { msgs.push("group by expression"); }
@after { msgs.pop(); }
    :
    expression
    ;

havingClause
@init { msgs.push("having clause"); }
@after { msgs.pop(); }
    :
    KW_HAVING havingCondition -> ^(TOK_HAVING havingCondition)
    ;

havingCondition
@init { msgs.push("having condition"); }
@after { msgs.pop(); }
    :
    expression
    ;

// order by a,b
orderByClause
@init { msgs.push("order by clause"); }
@after { msgs.pop(); }
    :
    KW_ORDER KW_BY
    LPAREN columnRefOrder
    ( COMMA columnRefOrder)* RPAREN -> ^(TOK_ORDERBY columnRefOrder+)
    |
    KW_ORDER KW_BY
    columnRefOrder
    ( COMMA columnRefOrder)* -> ^(TOK_ORDERBY columnRefOrder+)
    ;

clusterByClause
@init { msgs.push("cluster by clause"); }
@after { msgs.pop(); }
    :
    KW_CLUSTER KW_BY
    LPAREN expression (COMMA expression)* RPAREN -> ^(TOK_CLUSTERBY expression+)
    |
    KW_CLUSTER KW_BY
    expression
    ( COMMA expression )* -> ^(TOK_CLUSTERBY expression+)
    ;

distributeByClause
@init { msgs.push("distribute by clause"); }
@after { msgs.pop(); }
    :
    KW_DISTRIBUTE KW_BY
    LPAREN expression (COMMA expression)* RPAREN -> ^(TOK_DISTRIBUTEBY expression+)
    |
    KW_DISTRIBUTE KW_BY
    expression (COMMA expression)* -> ^(TOK_DISTRIBUTEBY expression+)
    ;

sortByClause
@init { msgs.push("sort by clause"); }
@after { msgs.pop(); }
    :
    KW_SORT KW_BY
    LPAREN columnRefOrder
    ( COMMA columnRefOrder)* RPAREN -> ^(TOK_SORTBY columnRefOrder+)
    |
    KW_SORT KW_BY
    columnRefOrder
    ( COMMA columnRefOrder)* -> ^(TOK_SORTBY columnRefOrder+)
    ;

// fun(par1, par2, par3)
function
@init { msgs.push("function specification"); }
@after { msgs.pop(); }
    :
    functionName
    LPAREN
      (
        (star=STAR)
        | (dist=KW_DISTINCT)? (expression (COMMA expression)*)?
      )
    RPAREN -> {$star != null}? ^(TOK_FUNCTIONSTAR functionName)
           -> {$dist == null}? ^(TOK_FUNCTION functionName (expression+)?)
                            -> ^(TOK_FUNCTIONDI functionName (expression+)?)
    ;

functionName
@init { msgs.push("function name"); }
@after { msgs.pop(); }
    : // Keyword IF is also a function name
    Identifier | KW_IF | KW_ARRAY | KW_MAP | KW_STRUCT | KW_UNIONTYPE
    ;

castExpression
@init { msgs.push("cast expression"); }
@after { msgs.pop(); }
    :
    KW_CAST
    LPAREN
          expression
          KW_AS
          primitiveType
    RPAREN -> ^(TOK_FUNCTION primitiveType expression)
    ;

caseExpression
@init { msgs.push("case expression"); }
@after { msgs.pop(); }
    :
    KW_CASE expression
    (KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_CASE expression*)
    ;

whenExpression
@init { msgs.push("case expression"); }
@after { msgs.pop(); }
    :
    KW_CASE
     ( KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_WHEN expression*)
    ;

constant
@init { msgs.push("constant"); }
@after { msgs.pop(); }
    :
    Number
    | StringLiteral
    | stringLiteralSequence
    | BigintLiteral
    | SmallintLiteral
    | TinyintLiteral
    | charSetStringLiteral
    | booleanValue
    ;

stringLiteralList
    :
    StringLiteral (COMMA StringLiteral)* -> ^(TOK_STRINGLITERALLIST StringLiteral+)
    ;


stringLiteralSequence
    :
    StringLiteral StringLiteral+ -> ^(TOK_STRINGLITERALSEQUENCE StringLiteral StringLiteral+)
    ;

charSetStringLiteral
@init { msgs.push("character string literal"); }
@after { msgs.pop(); }
    :
    csName=CharSetName csLiteral=CharSetLiteral -> ^(TOK_CHARSETLITERAL $csName $csLiteral)
    ;

expression
@init { msgs.push("expression specification"); }
@after { msgs.pop(); }
    :
    precedenceOrExpression
    ;

atomExpression
    :
    KW_NULL -> TOK_NULL
    | constant
    | function
    | castExpression
    | caseExpression
    | whenExpression
    | tableOrColumn
    | LPAREN! expression RPAREN!
    ;


precedenceFieldExpression
    :
    atomExpression ((LSQUARE^ expression RSQUARE!) | (DOT^ Identifier))*
    ;

precedenceUnaryOperator
    :
    PLUS | MINUS | TILDE
    ;

nullCondition
    :
    KW_NULL -> ^(TOK_ISNULL)
    | KW_NOT KW_NULL -> ^(TOK_ISNOTNULL)
    ;

precedenceUnaryPrefixExpression
    :
    (precedenceUnaryOperator^)* precedenceFieldExpression
    ;

precedenceUnarySuffixExpression
    : precedenceUnaryPrefixExpression (a=KW_IS nullCondition)?
    -> {$a != null}? ^(TOK_FUNCTION nullCondition precedenceUnaryPrefixExpression)
    -> precedenceUnaryPrefixExpression
    ;


precedenceBitwiseXorOperator
    :
    BITWISEXOR
    ;

precedenceBitwiseXorExpression
    :
    precedenceUnarySuffixExpression (precedenceBitwiseXorOperator^ precedenceUnarySuffixExpression)*
    ;


precedenceStarOperator
    :
    STAR | DIVIDE | MOD | DIV
    ;

precedenceStarExpression
    :
    precedenceBitwiseXorExpression (precedenceStarOperator^ precedenceBitwiseXorExpression)*
    ;


precedencePlusOperator
    :
    PLUS | MINUS
    ;

precedencePlusExpression
    :
    precedenceStarExpression (precedencePlusOperator^ precedenceStarExpression)*
    ;


precedenceAmpersandOperator
    :
    AMPERSAND
    ;

precedenceAmpersandExpression
    :
    precedencePlusExpression (precedenceAmpersandOperator^ precedencePlusExpression)*
    ;


precedenceBitwiseOrOperator
    :
    BITWISEOR
    ;

precedenceBitwiseOrExpression
    :
    precedenceAmpersandExpression (precedenceBitwiseOrOperator^ precedenceAmpersandExpression)*
    ;


// Equal operators supporting NOT prefix
precedenceEqualNegatableOperator
    :
    KW_LIKE | KW_RLIKE | KW_REGEXP
    ;

precedenceEqualOperator
    :
    precedenceEqualNegatableOperator | EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;

precedenceEqualExpression
    :
    (left=precedenceBitwiseOrExpression -> $left)
    (
       (KW_NOT precedenceEqualNegatableOperator notExpr=precedenceBitwiseOrExpression)
       -> ^(KW_NOT ^(precedenceEqualNegatableOperator $precedenceEqualExpression $notExpr))
    | (precedenceEqualOperator equalExpr=precedenceBitwiseOrExpression)
       -> ^(precedenceEqualOperator $precedenceEqualExpression $equalExpr)
    | (KW_NOT KW_IN expressions)
       -> ^(KW_NOT ^(TOK_FUNCTION KW_IN $precedenceEqualExpression expressions))
    | (KW_IN expressions)
       -> ^(TOK_FUNCTION KW_IN $precedenceEqualExpression expressions)
    | ( KW_NOT KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
       -> ^(TOK_FUNCTION Identifier["between"] KW_TRUE $left $min $max)
    | ( KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
       -> ^(TOK_FUNCTION Identifier["between"] KW_FALSE $left $min $max)
    )*
    ;

expressions
    :
    LPAREN expression (COMMA expression)* RPAREN -> expression*
    ;

precedenceNotOperator
    :
    KW_NOT
    ;

precedenceNotExpression
    :
    (precedenceNotOperator^)* precedenceEqualExpression
    ;


precedenceAndOperator
    :
    KW_AND
    ;

precedenceAndExpression
    :
    precedenceNotExpression (precedenceAndOperator^ precedenceNotExpression)*
    ;


precedenceOrOperator
    :
    KW_OR
    ;

precedenceOrExpression
    :
    precedenceAndExpression (precedenceOrOperator^ precedenceAndExpression)*
    ;


booleanValue
    :
    KW_TRUE^ | KW_FALSE^
    ;

tableOrPartition
   :
   tableName partitionSpec? -> ^(TOK_TAB tableName partitionSpec?)
   ;

partitionSpec
    :
    KW_PARTITION
     LPAREN partitionVal (COMMA  partitionVal )* RPAREN -> ^(TOK_PARTSPEC partitionVal +)
    ;

partitionVal
    :
    Identifier (EQUAL constant)? -> ^(TOK_PARTVAL Identifier constant?)
    ;

dropPartitionSpec
    :
    KW_PARTITION
     LPAREN dropPartitionVal (COMMA  dropPartitionVal )* RPAREN -> ^(TOK_PARTSPEC dropPartitionVal +)
    ;

dropPartitionVal
    :
    Identifier dropPartitionOperator constant -> ^(TOK_PARTVAL Identifier dropPartitionOperator constant)
    ;

dropPartitionOperator
    :
    EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;

sysFuncNames
    :
      KW_AND
    | KW_OR
    | KW_NOT
    | KW_LIKE
    | KW_IF
    | KW_CASE
    | KW_WHEN
    | KW_TINYINT
    | KW_SMALLINT
    | KW_INT
    | KW_BIGINT
    | KW_FLOAT
    | KW_DOUBLE
    | KW_BOOLEAN
    | KW_STRING
    | KW_BINARY
    | KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    | KW_UNIONTYPE
    | EQUAL
    | EQUAL_NS
    | NOTEQUAL
    | LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    | DIVIDE
    | PLUS
    | MINUS
    | STAR
    | MOD
    | DIV
    | AMPERSAND
    | TILDE
    | BITWISEOR
    | BITWISEXOR
    | KW_RLIKE
    | KW_REGEXP
    | KW_IN
    | KW_BETWEEN

    ;

descFuncNames
    :
      sysFuncNames
    | StringLiteral
    | Identifier
    ;

// Keywords
kwRole
:
{input.LT(1).getText().equalsIgnoreCase("role")}? Identifier;
//'ROLE';

kwInner
:
{input.LT(1).getText().equalsIgnoreCase("inner")}? Identifier;

kwUsers				//added by liulichao
:
//{input.LT(1).getText().equalsIgnoreCase("users")}?;
'USERS';

kwUser				//added by liulichao
:'USER';
//{input.LT(1).getText().equalsIgnoreCase("user")}?;
//{input.LT(1).getText().equalsIgnoreCase("user")}? Identifier;

KW_TRUE : 'TRUE';
KW_FALSE : 'FALSE';
KW_ALL : 'ALL';
KW_AND : 'AND';
KW_OR : 'OR';
KW_NOT : 'NOT' | '!';
KW_LIKE : 'LIKE';

KW_IF : 'IF';
KW_EXISTS : 'EXISTS';

KW_ASC : 'ASC';
KW_DESC : 'DESC';
KW_ORDER : 'ORDER';
KW_GROUP : 'GROUP';
KW_BY : 'BY';
KW_HAVING : 'HAVING';
KW_WHERE : 'WHERE';
KW_FROM : 'FROM';
KW_AS : 'AS';
KW_SELECT : 'SELECT';
KW_DISTINCT : 'DISTINCT';
KW_INSERT : 'INSERT';
KW_OVERWRITE : 'OVERWRITE';
KW_OUTER : 'OUTER';
KW_UNIQUEJOIN : 'UNIQUEJOIN';
KW_PRESERVE : 'PRESERVE';
KW_JOIN : 'JOIN';
KW_LEFT : 'LEFT';
KW_RIGHT : 'RIGHT';
KW_FULL : 'FULL';
KW_ON : 'ON';
KW_SPLIT : 'SPLIT';
KW_PARTITION : 'PARTITION';
KW_PARTITIONS : 'PARTITIONS';
KW_TABLE: 'TABLE';
KW_TABLES: 'TABLES';
KW_COLUMNS: 'COLUMNS';
KW_INDEX: 'INDEX';
KW_INDEXES: 'INDEXES';
KW_REBUILD: 'REBUILD';
KW_FUNCTIONS: 'FUNCTIONS';
KW_SHOW: 'SHOW';
KW_MSCK: 'MSCK';
KW_REPAIR: 'REPAIR';
KW_DIRECTORY: 'DIRECTORY';
KW_LOCAL: 'LOCAL';
KW_TRANSFORM : 'TRANSFORM';
KW_USING: 'USING';
KW_CLUSTER: 'CLUSTER';
KW_DISTRIBUTE: 'DISTRIBUTE';
KW_SORT: 'SORT';
KW_UNION: 'UNION';
KW_LOAD: 'LOAD';
KW_EXPORT: 'EXPORT';
KW_IMPORT: 'IMPORT';
KW_DATA: 'DATA';
KW_INPATH: 'INPATH';
KW_IS: 'IS';
KW_NULL: 'NULL';
KW_CREATE: 'CREATE';
KW_EXTERNAL: 'EXTERNAL';
KW_ALTER: 'ALTER';
KW_CHANGE: 'CHANGE';
KW_COLUMN: 'COLUMN';
KW_FIRST: 'FIRST';
KW_AFTER: 'AFTER';
KW_DESCRIBE: 'DESCRIBE';
KW_DROP: 'DROP';
KW_RENAME: 'RENAME';
KW_TO: 'TO';
KW_COMMENT: 'COMMENT';
KW_BOOLEAN: 'BOOLEAN';
KW_TINYINT: 'TINYINT';
KW_SMALLINT: 'SMALLINT';
KW_INT: 'INT';
KW_BIGINT: 'BIGINT';
KW_FLOAT: 'FLOAT';
KW_DOUBLE: 'DOUBLE';
KW_DATE: 'DATE';
KW_DATETIME: 'DATETIME';
KW_TIMESTAMP: 'TIMESTAMP';
KW_STRING: 'STRING';
KW_ARRAY: 'ARRAY';
KW_STRUCT: 'STRUCT';
KW_MAP: 'MAP';
KW_UNIONTYPE: 'UNIONTYPE';
KW_REDUCE: 'REDUCE';
KW_SPLITED: 'SPLITED';
KW_PARTITIONED: 'PARTITIONED';
KW_CLUSTERED: 'CLUSTERED';
KW_SORTED: 'SORTED';
KW_INTO: 'INTO';
KW_BUCKETS: 'BUCKETS';
KW_ROW: 'ROW';
KW_FORMAT: 'FORMAT';
KW_DELIMITED: 'DELIMITED';
KW_FIELDS: 'FIELDS';
KW_TERMINATED: 'TERMINATED';
KW_ESCAPED: 'ESCAPED';
KW_COLLECTION: 'COLLECTION';
KW_ITEMS: 'ITEMS';
KW_KEYS: 'KEYS';
KW_KEY_TYPE: '$KEY$';
KW_LINES: 'LINES';
KW_STORED: 'STORED';
KW_FILEFORMAT: 'FILEFORMAT';
KW_SEQUENCEFILE: 'SEQUENCEFILE';
KW_TEXTFILE: 'TEXTFILE';
KW_RCFILE: 'RCFILE';
KW_INPUTFORMAT: 'INPUTFORMAT';
KW_OUTPUTFORMAT: 'OUTPUTFORMAT';
KW_INPUTDRIVER: 'INPUTDRIVER';
KW_OUTPUTDRIVER: 'OUTPUTDRIVER';
KW_OFFLINE: 'OFFLINE';
KW_ENABLE: 'ENABLE';
KW_DISABLE: 'DISABLE';
KW_READONLY: 'READONLY';
KW_NO_DROP: 'NO_DROP';
KW_LOCATION: 'LOCATION';
KW_TABLESAMPLE: 'TABLESAMPLE';
KW_BUCKET: 'BUCKET';
KW_OUT: 'OUT';
KW_OF: 'OF';
KW_PERCENT: 'PERCENT';
KW_CAST: 'CAST';
KW_ADD: 'ADD';
KW_NEW:'NEW';
KW_REPLACE: 'REPLACE';
KW_RLIKE: 'RLIKE';
KW_REGEXP: 'REGEXP';
KW_TEMPORARY: 'TEMPORARY';
KW_FUNCTION: 'FUNCTION';
KW_EXPLAIN: 'EXPLAIN';
KW_EXTENDED: 'EXTENDED';
KW_FORMATTED: 'FORMATTED';
KW_DEPENDENCY: 'DEPENDENCY';
KW_SERDE: 'SERDE';
KW_WITH: 'WITH';
KW_DEFERRED: 'DEFERRED';
KW_SERDEPROPERTIES: 'SERDEPROPERTIES';
KW_DBPROPERTIES: 'DBPROPERTIES';
KW_LIMIT: 'LIMIT';
KW_SET: 'SET';
KW_TBLPROPERTIES: 'TBLPROPERTIES';
KW_IDXPROPERTIES: 'IDXPROPERTIES';
KW_VALUE_TYPE: '$VALUE$';
KW_ELEM_TYPE: '$ELEM$';
KW_CASE: 'CASE';
KW_WHEN: 'WHEN';
KW_THEN: 'THEN';
KW_ELSE: 'ELSE';
KW_END: 'END';
KW_MAPJOIN: 'MAPJOIN';
KW_STREAMTABLE: 'STREAMTABLE';
KW_HOLD_DDLTIME: 'HOLD_DDLTIME';
KW_CLUSTERSTATUS: 'CLUSTERSTATUS';
KW_UTC: 'UTC';
KW_UTCTIMESTAMP: 'UTC_TMESTAMP';
KW_LONG: 'LONG';
KW_DELETE: 'DELETE';
KW_PLUS: 'PLUS';
KW_MINUS: 'MINUS';
KW_FETCH: 'FETCH';
KW_INTERSECT: 'INTERSECT';
KW_VIEW: 'VIEW';
KW_IN: 'IN';
KW_DATABASE: 'DATABASE';
KW_DATABASES: 'DATABASES';
KW_MATERIALIZED: 'MATERIALIZED';
KW_SCHEMA: 'SCHEMA';
KW_SCHEMAS: 'SCHEMAS';
KW_GRANT: 'GRANT';
KW_REVOKE: 'REVOKE';
KW_SSL: 'SSL';
KW_UNDO: 'UNDO';
KW_LOCK: 'LOCK';
KW_LOCKS: 'LOCKS';
KW_UNLOCK: 'UNLOCK';
KW_SHARED: 'SHARED';
KW_EXCLUSIVE: 'EXCLUSIVE';
KW_PROCEDURE: 'PROCEDURE';
KW_UNSIGNED: 'UNSIGNED';
KW_WHILE: 'WHILE';
KW_READ: 'READ';
KW_READS: 'READS';
KW_PURGE: 'PURGE';
KW_RANGE: 'RANGE';
KW_ANALYZE: 'ANALYZE';
KW_BEFORE: 'BEFORE';
KW_BETWEEN: 'BETWEEN';
KW_BOTH: 'BOTH';
KW_BINARY: 'BINARY';
KW_BLOB : 'BLOB';
KW_CROSS: 'CROSS';
KW_CONTINUE: 'CONTINUE';
KW_CURSOR: 'CURSOR';
KW_TRIGGER: 'TRIGGER';
KW_RECORDREADER: 'RECORDREADER';
KW_RECORDWRITER: 'RECORDWRITER';
KW_SEMI: 'SEMI';
KW_LATERAL: 'LATERAL';
KW_TOUCH: 'TOUCH';
KW_ARCHIVE: 'ARCHIVE';
KW_UNARCHIVE: 'UNARCHIVE';
KW_COMPUTE: 'COMPUTE';
KW_STATISTICS: 'STATISTICS';
KW_USE: 'USE';
KW_OPTION: 'OPTION';
KW_CONCATENATE: 'CONCATENATE';
KW_SHOW_DATABASE: 'SHOW_DATABASE';
KW_UPDATE: 'UPDATE';
KW_RESTRICT: 'RESTRICT';
KW_CASCADE: 'CASCADE';
KW_SKEWED: 'SKEWED';
KW_ROLLUP: 'ROLLUP';
KW_CUBE: 'CUBE';
KW_DIRECTORIES: 'DIRECTORIES';
KW_FOR: 'FOR';
KW_GROUPING: 'GROUPING';
KW_SETS: 'SETS';

KW_DBA: 'DBA';	//added by liulichao,begin
KW_SETPASSWD: 'SETPASSWD';
KW_IDENTIFIED: 'IDENTIFIED';
KW_CONNECT: 'CONNECT';	//added by liulichao,end
//added by zjw
KW_VALUES:	 'VALUES';
KW_SUBSPLITED: 'SUBSPLITED';
KW_SUBSPLIT: 'SUBSPLIT';
KW_SUBPARTITIONED: 'SUBPARTITIONED';
KW_SUBPARTITION: 'SUBPARTITION';
KW_LESS:'LESS';
KW_GREATER:'GREATER';
KW_THAN:'THAN';
KW_DW:'DATAWAREHOUSE';
KW_DIRECT:'DIRECT';

KW_NODE:'NODE';
KW_NODEPROPERTIES:'NODEPROPERTIES';
KW_MODIFY:'MODIFY';
KW_FILE:'FILE';
KW_SUBPARTITIONS:'SUBPARTITIONS';
KW_HETER: 'HETER';
KW_PARTITION_KEYS: 'PARTITION_KEYS';
KW_BUSITYPES:'BUSITYPES';
KW_BUSITYPE:'BUSITYPE';
KW_NODES:'NODES';
KW_FILES:'FILES';
KW_FILELOCATIONS:'FILELOCATIONS';
KW_NODEGROUP:'NODEGROUP';
KW_NODEGROUPS:'NODEGROUPS';
KW_NGPROPERTIES:'NGPROPERTIES';
KW_GEOLOC:'GEOLOC';
KW_EQROOM:'EQROOM';
KW_NODEASSIGNMENT:'NODEASSIGNMENT';
KW_NODEGROUPASSIGNMENT:'NODEGROUPASSIGNMENT';
KW_USERASSIGNMENT:'USERASSIGNMENT';	
KW_ROLEASSIGNMENT:'ROLEASSIGNMENT';
KW_SCHEMAPROPERTIES:	'SCHEMEPROPERTIES';

// Operators
// NOTE: if you add a new function/operator, add it to sysFuncNames so that describe function _FUNC_ will work.

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';

EQUAL : '=' | '==';
EQUAL_NS : '<=>';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

DIVIDE : '/';
PLUS : '+';
MINUS : '-';
STAR : '*';
MOD : '%';
DIV : 'DIV';

AMPERSAND : '&';
TILDE : '~';
BITWISEOR : '|';
BITWISEXOR : '^';
QUESTION : '?';
DOLLAR : '$';

// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
HexDigit
    : 'a'..'f' | 'A'..'F'
    ;

fragment
Digit
    :
    '0'..'9'
    ;

fragment
Exponent
    :
    ('e' | 'E') ( PLUS|MINUS )? (Digit)+
    ;

fragment
RegexComponent
    : 'a'..'z' | 'A'..'Z' | '0'..'9' | '_'
    | PLUS | STAR | QUESTION | MINUS | DOT
    | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY
    | BITWISEXOR | BITWISEOR | DOLLAR
    ;

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    )+
    ;

CharSetLiteral
    :
    StringLiteral
    | '0' 'X' (HexDigit|Digit)+
    ;

BigintLiteral
    :
    (Digit)+ 'L'
    ;

SmallintLiteral
    :
    (Digit)+ 'S'
    ;

TinyintLiteral
    :
    (Digit)+ 'Y'
    ;

Number
    :
    (Digit)+ ( DOT (Digit)* (Exponent)? | Exponent)?
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_')*
    | '`' RegexComponent+ '`'
    ;

CharSetName
    :
    '_' (Letter | Digit | '_' | '-' | '.' | ':' )+
    ;

WS  :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}
    ;

COMMENT
  : '--' (~('\n'|'\r'))*
    { $channel=HIDDEN; }
  ;




