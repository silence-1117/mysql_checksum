package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"regexp"
	"sync"
	"time"
)
var (
	mConChan chan *sql.DB
 	sConChan chan *sql.DB
 	wait = sync.WaitGroup{}
	metadata map[string] string = make(map[string]string)
	checkResult map[string] int64 =  make(map[string]int64)
	file,gtidSet,gtidExecuted = "","",""
    pos = 0
)

func initFTWRL(mDbUrl string,sDbUrl string,conNum int)  {
	db, err := sql.Open("mysql",mDbUrl)
	if err != nil {
		fmt.Printf("connection dbUrl failed: %v \n",err)
		return
	}
	defer db.Close()

	//exec flush tables
	_, err = db.Exec("set session lock_wait_timeout=10")
	if err != nil {
		fmt.Printf("exec set session lock_wait_timeout=10 failed,exit , %v\n",err)
		return
	}

	//exec flush tables
	_, err = db.Exec("FLUSH /*!40101 LOCAL */ TABLES")
	if err != nil {
		fmt.Printf("exec FLUSH /*!40101 LOCAL */ TABLES failed,exit , %v\n",err)
		return
	}

	//get schema information

	//exec flush table with read lock
	_, err = db.Exec("FLUSH TABLES WITH READ LOCK")
	if err != nil {
		fmt.Printf("exec FLUSH TABLES WITH READ LOCK failed,exit , %v\n",err)
		return
	}

	wait.Add(1)
	go getMetadata(mDbUrl)

	//get gtid_executed
	res, err := db.Query("SELECT @@GLOBAL.GTID_EXECUTED")
	if err != nil {
		fmt.Printf("exec SELECT @@GLOBAL.GTID_EXECUTED failed,exit , %v\n",err)
		return
	}
	for res.Next() {
		res.Scan(&gtidExecuted)
	}

	//get binlog position
	res, err = db.Query("SHOW MASTER STATUS")
	if err != nil {
		fmt.Printf("exec SHOW MASTER STATUS failed, exit... %v\n",err)
		return
	}

	var do,ignore = "",""
	for res.Next() {
		res.Scan(&file,&pos,&do,&ignore,&gtidSet)
	}

	wait.Add(1)
	go initSlavePos(sDbUrl)

	wait.Add(1)
	go initPool(mDbUrl,true,conNum)

	//get gtid_executed
	_, err = db.Exec("UNLOCK TABLES")
	if err != nil {
		fmt.Printf("exec UNLOCK TABLES failed,exit , %v\n",err)
		return
	}
	if slaveIsRun(sDbUrl) == 0 {
		wait.Add(1)
		go initPool(sDbUrl,false,conNum)
	}

	for i := 0 ; i < 100 ; i++ {
		//fmt.Printf("%v:%v\n",len(master_con),len(slave_con))
		if len(mConChan) == conNum && len(sConChan) == conNum {
			wait.Add(1)
			go startChecksum(mDbUrl,sDbUrl)
			wait.Add(1)
			go startSlave(sDbUrl)
			break
		}
		time.Sleep(time.Millisecond*100)
	}
	wait.Wait()
}

func startChecksum(mDbUrl string,sDbUrl string)  {
	defer wait.Done()
	reg := regexp.MustCompile(`.*:.*@\(([\d]{1,3}\.[\d]{1,3}\.[\d]{0,3}\.[\d]{1,3}:[\d]{4,6})\)/.*`)
	mHostPort := reg.FindStringSubmatch(mDbUrl)[1]
	sHostPort := reg.FindStringSubmatch(sDbUrl)[1]
	//var mTable,sTable = "",""
	for table := range metadata {
		//fmt.Println("m:",table)
		wait.Add(1)
		go queryRet(mHostPort,true,table,3)
		wait.Add(1)
		go queryRet(sHostPort,false,table,3)
	}
}

func queryRet(hostPort string,isMaster bool,table string,retryNum int) {
	defer wait.Done()
	var sum int64 = 0
	var db *sql.DB
	for i := 0; i < retryNum;i++ {
		if isMaster{
			db = <- mConChan
		} else {
			db = <- sConChan
		}
		res,err := db.Query(fmt.Sprintf("CHECKSUM TABLE %s",table))
		if err != nil {
			fmt.Printf("checksum table %s failed,... %v\n",table,err)
			continue
		}
		for res.Next() {
			res.Scan(&table,&sum)
			fmt.Println("table:",table,sum)
			checkResult[fmt.Sprintf("%s:%s",hostPort,table)] = sum
		}
		_, err = db.Exec("ROLLBACK TO SAVEPOINT sp")
		if err != nil {
			fmt.Printf("%s exec ROLLBACK TO SAVEPOINT sp failed, exit... %v\n",hostPort,err)
			return
		}
		if isMaster {
			mConChan <- db
		} else {
			sConChan <- db
		}
		break
	}
}

func initPool(dbUrl string,isMaster bool,poolNum int)  {
	defer wait.Done()
	if isMaster {
		mConChan = make(chan *sql.DB,poolNum)
	} else {
		sConChan = make(chan *sql.DB,poolNum)
	}
	for i := 0; i < poolNum; i++ {
		db, err := sql.Open("mysql",dbUrl)
		if err != nil {
			fmt.Printf("connection dbUrl failed: %v \n",err)
			return
		}
		err = db.Ping()
		if err != nil {
			fmt.Printf("init connection failed,%v\n",err)
		}

		_, err = db.Exec("SET SESSION wait_timeout = 3600")
		if err != nil {
			fmt.Printf("exec SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ failed, exit... %v\n",err)
			return
		}

		_, err = db.Exec("SET SESSION interactive_timeout = 3600")
		if err != nil {
			fmt.Printf("exec SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ failed, exit... %v\n",err)
			return
		}

		_, err = db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if err != nil {
			fmt.Printf("exec SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ failed, exit... %v\n",err)
			return
		}

		_, err = db.Exec("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */")
		if err != nil {
			fmt.Printf("exec start transaction failed, exit... %v\n",err)
			return
		}
		_, err = db.Exec("SAVEPOINT sp")
		if err != nil {
			fmt.Printf("%s exec start transaction failed, exit... %v\n",dbUrl,err)
			return
		}
		if isMaster {
			mConChan <- db
		} else {
			sConChan <- db
		}
	}

}

func slaveIsRun(dbUrl string) int {
	db, err := sql.Open("mysql",dbUrl)
	if err != nil {
		fmt.Printf("connection dbUrl failed: %v \n",err)
		return -1
	}
	defer db.Close()
	var slaveRun int = 1
	for i := 0; i < 100;i++ {
		res, err := db.Query("SELECT IF(VARIABLE_VALUE='ON',1,0) RUN FROM INFORMATION_SCHEMA.GLOBAL_STATUS WHERE VARIABLE_NAME = 'SLAVE_RUNNING'")
		if err != nil {
			fmt.Printf("exec GET SLAVE failed, exit... %v\n",err)
			return -1
		}
		for res.Next() {
			res.Scan(&slaveRun)
		}
		if slaveRun == 0 {
			return slaveRun
		}
		time.Sleep(time.Second*1)
	}

	return slaveRun
}
func startSlave(dbUrl string)  {
	defer wait.Done()
	db, err := sql.Open("mysql",dbUrl)
	if err != nil {
		fmt.Printf("conn failed: %v \n",err)
		return
	}
	defer db.Close()
	_, err = db.Exec("START SLAVE")
	if err != nil {
		fmt.Printf("exec START SLAVE failed, exit... %v\n",err)
		return
	}
}
func initSlavePos(dbUrl string)  {
	defer wait.Done()
	db, err := sql.Open("mysql",dbUrl)
	if err != nil {
		fmt.Printf("connection dbUrl failed: %v \n",err)
		return
	}
	defer db.Close()

	//stop slave in master file and  position
	_, err = db.Exec("STOP SLAVE;")
	if err != nil {
		fmt.Printf("exec STOP SLAVE failed,exit , %v\n",err)
		return
	}
	stopPos := fmt.Sprintf("START SLAVE UNTIL MASTER_LOG_FILE = '%s', MASTER_LOG_POS = %d",file,pos)
	_, err = db.Exec(stopPos)
	if err != nil {
		fmt.Printf("exec %s,exit , %v\n",stopPos,err)
		return
	}

}
func getMetadata(dbUrl string)  {
	defer wait.Done()
	getSchema := "select table_schema,table_name from information_schema.tables where table_schema not in('mysql','sys','performance_schema','information_schema') and TABLE_TYPE='BASE TABLE'"
	//getSchema := "select table_schema,table_name from information_schema.tables where table_schema  in('table_size') and TABLE_TYPE='BASE TABLE'"

	db, err := sql.Open("mysql",dbUrl)
	if err != nil {
		fmt.Printf("conn failed: %v \n",err)
		return
	}
	defer db.Close()
	res, err := db.Query(getSchema)
	if err != nil {
		fmt.Printf("exec get schema information failed, exit... %v\n",err)
		return
	}
	var database,table = "",""
	for res.Next() {
		res.Scan(&database,&table)
		key := fmt.Sprintf("%s.%s",database,table)
		metadata[key] = key
	}
}

func checksumResult(mDbUrl string,sDbUrl string)  {
	reg := regexp.MustCompile(`.*:.*@\(([\d]{1,3}\.[\d]{1,3}\.[\d]{0,3}\.[\d]{1,3}:[\d]{4,6})\)/.*`)
	mHostPort := reg.FindStringSubmatch(mDbUrl)[1]
	sHostPort := reg.FindStringSubmatch(sDbUrl)[1]
	var mTable,sTable = "",""
	for table := range metadata {
		mTable = fmt.Sprintf("%s:%s",mHostPort,table)
		sTable = fmt.Sprintf("%s:%s",sHostPort,table)
		if checkResult[mTable] == checkResult[sTable] {
			fmt.Printf("%s\t%s\t%d\tPASS\n",mTable,sTable,checkResult[mTable])
		} else {
			fmt.Printf("%s:%d\t%s:%d\tINCONSISTENT\n",mTable,checkResult[mTable],sTable,checkResult[sTable])
		}
	}
}

func closeAll()  {
	close(mConChan)
	close(sConChan)
	for db := range mConChan  {
		db.Close()
	}
	for db := range sConChan {
		db.Close()
	}

}
func main()  {
	mDbUrl := "test1:test@(127.0.0.1:5723)/mysql?charset=utf8"
	sDbUrl := "test1:test@(127.0.0.1:5724)/mysql?charset=utf8"

	initFTWRL(mDbUrl,sDbUrl,4)
	checksumResult(mDbUrl,sDbUrl)
	defer closeAll()
}
