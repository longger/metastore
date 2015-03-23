#!/bin/bash

#java [-cp classpath] [system_props] org.datanucleus.store.rdbms.SchemaTool [modes] [options] [props] 
#                    [jdo-files] [class-files]
#    where system_props (when specified) should include
#        -Ddatanucleus.ConnectionDriverName=db_driver_name
#        -Ddatanucleus.ConnectionURL=db_url
#        -Ddatanucleus.ConnectionUserName=db_username
#        -Ddatanucleus.ConnectionPassword=db_password
#        -Ddatanucleus.Mapping=orm_mapping_name (optional)
#        -Dlog4j.configuration=file:{log4j.properties} (optional)
#    where modes can be
#        -create : Create the tables specified by the jdo-files/class-files
#        -delete : Delete the tables specified by the jdo-files/class-files
#        -validate : Validate the tables specified by the jdo-files/class-files
#        -dbinfo : Detailed information about the database
#        -schemainfo : Detailed information about the database schema
#    where options can be
#        -ddlFile {filename} : only for use with "create" mode to dump the DDL to the specified file
#        -completeDdl : when using "ddlFile" to get all DDL output and not just missing tables/constraints
#        -api : The API that is being used (default is JDO, but can be set to JPA)
#        -pu {persistence-unit-name} : Name of the persistence unit to manage the schema for
#        -v : verbose output
#    where props can be
#        -props {propsfilename} : PMF properties to use in place of the "system_props"

CPPATH=/tmp/lib
for f in $CPPATH/*.jar; do
	XLIBS=$XLIBS:$f;
done
for c in ~/workspace/hive-0.10.0/src/build/metastore/classes/org/apache/hadoop/hive/metastore/model/M*; do
	XLIBS=$XLIBS:$c;
	CS="$CS $c";
done

echo $XLIBS; exit
XLIBS=$XLIBS:~/workspace/hive-0.10.0/src/build/metastore/classes/
#CLASSPATH=$CPPATH/datanucleus-core-3.2.2.jar:$CPPATH/datanucleus-rdbms-3.2.1.jar:$CPPATH/datanucleus-api-jdo-3.2.1.jar java \
#    -Ddatanucleus.ConnectionDriverName=com.oscar.Driver \
#    -Ddatanucleus.ConnectionURL=jdbc:oscar://192.168.1.19:2003/metastore \
java -cp $XLIBS \
    -Ddatanucleus.ConnectionDriverName=oracle.jdbc.driver.OracleDriver \
    -Ddatanucleus.ConnectionURL=jdbc:oracle:thin:@192.168.1.13:1521/metastore \
    -Ddatanucleus.ConnectionUserName=meta \
    -Ddatanucleus.ConnectionPassword=meta \
    org.datanucleus.store.schema.SchemaTool \
	-create \
    -ddlFile ./abc.sql \
    -v -completeDdl \
    ~/workspace/hive-0.10.0/src/metastore/src/model/package.jdo
