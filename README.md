# mysql_checksum
mysql checksum
MySQL master-slave overall data consistency verification, temporarily does not include incremental data verification

The working principle is as follows

step1: FLUSH TABLES WITH READ LOCK

step2: SHOW MASTER STATUS && SELECT @@GLOBAL.GTID_EXECUTED

step4: init pool
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ
START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */
conChan <- con

step5: checksum table table_name


数据一致性检查原理：主要依赖MySQL自带的快照事务及checksum table命令来实现，主要实现步骤如下

步骤1：执行FLUSH TABLES WITH READ LOCK，获取全局一致性位点

步骤2：获取binlog位点信息SHOW MASTER STATUS && SELECT @@GLOBAL.GTID_EXECUTED

步骤3：初始化主从数据库连接池，主从执行SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ && SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ，获取一致性位点的连接放入主从连接池chan

步骤4：通过连接池chan实现并发checksum
