package main

import (
	"fmt"
	"time"

	"github.com/Centny/gwf/dbutil"

	"github.com/Centny/gwf/tutil"

	"github.com/Centny/dbm/sql"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// mysql.RegisterDial("tcp", dial mysql.DialFunc)
	// sql.Default.Balanced = false
	// sql.AddDefault2("mysql", "testx:123@tcp(192.168.2.248:3306,192.168.2.249:3306)/testx?wsrep_sync_wait=3&charset=utf8&loc=Local&interpolateParams=true")
	sql.AddDefault2("mysql", "testx:123@tcp(192.168.2.248:3306,192.168.2.249:3306)/testx?charset=utf8&loc=Local&interpolateParams=true")
	time.Sleep(3 * time.Second)
	sql.Db().Exec("DELETE FROM TESTX_USER")
	perf := tutil.NewPerf()
	perf.ShowState = true
	perf.AutoExec(1000000, 100, 1, "", 100, func(idx int, state tutil.Perf) error {
		return nil
	}, func(v int) error {
		db := sql.Db()
		tx, err := db.Begin()
		if err != nil {
			fmt.Printf("begin error:%v\n", err)
			return nil
		}
		_, err = dbutil.DbInsert2(tx, "INSERT INTO TESTX_USER (USERNAME,PASSWORD,STATUS) VALUES (?,?,?)",
			fmt.Sprintf("u_%v", v), fmt.Sprintf("p_%v", v), "N")
		if err != nil {
			fmt.Printf("insert error:%v\n", err)
			tx.Rollback()
			return nil
		}
		err = tx.Commit()
		if err != nil {
			fmt.Printf("commit error:%v\n", err)
			return nil
		}
		// time.Sleep(10 * time.Millisecond)
		db2 := sql.Db()
		// db2 := db
		count, err := dbutil.DbQueryI(db2, "SELECT COUNT(*) FROM TESTX_USER WHERE USERNAME=?", fmt.Sprintf("u_%v", v))
		if err != nil {
			fmt.Printf("query error:%v\n", err)
			return nil
		}
		if count < 1 {
			fmt.Printf("query count error->%v\n", v)
		}
		return nil
	})
	// fmt.Printf("Used:%v", a ...interface{})
	//ALTER TABLE `TESTX_USER` ADD INDEX `IDX_USERNAME` (`USERNAME` ASC);
}
