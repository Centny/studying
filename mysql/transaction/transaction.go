package main

import (
	"github.com/Centny/dbm/sql"
	"github.com/Centny/gwf/dbutil"
	"github.com/Centny/gwf/tutil"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// sql.AddDefault2("mysql", "cny:sco@tcp(loc.m:3306)/test?charset=utf8&loc=Local&interpolateParams=true")
	sql.Default.Balanced = false
	sql.AddDefault2("mysql", "testx:123@tcp(192.168.2.248:3306,192.168.2.249:3306)/testx?wsrep_sync_wait=3&charset=utf8&loc=Local&interpolateParams=true")
	test1()
}

func test1() {
	perf := tutil.NewPerf()
	perf.ShowState = true
	sql.Db().Exec("UPDATE TEST_VAL1 SET VALUE=10000")
	perf.AutoExec(1000, 100, 1, "", 100, func(idx int, state tutil.Perf) error {
		return nil
	}, func(v int) error {
		err := run1(1)
		if err != nil {
			panic(err)
		}
		return nil
	})
}

func run1(v1 int) (err error) {
	db := sql.Db()
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}()
	having, err := dbutil.DbQueryI2(tx, "SELECT VALUE FROM TEST_VAL1 WHERE TID=? FOR UPDATE", v1)
	// having, err := dbutil.DbQueryI2(tx, "SELECT VALUE FROM TEST_VAL1 WHERE TID=?", v1)
	if err != nil {
		return
	}
	if having > 10 {
		// fmt.Println("-->", having, old)
		_, err = tx.Exec("UPDATE TEST_VAL1 SET VALUE=? WHERE TID=?", having-10, v1)
		if err != nil {
			return
		}
	}
	return
}

func test2() {
	perf := tutil.NewPerf()
	perf.ShowState = true
	sql.Db().Exec("UPDATE TEST_VAL1 SET VALUE=10000")
	sql.Db().Exec("UPDATE TEST_VAL2 SET VALUE=0")
	perf.AutoExec(1000, 100, 1, "", 100, func(idx int, state tutil.Perf) error {
		return nil
	}, func(v int) error {
		err := run2(1)
		if err != nil {
			panic(err)
		}
		return nil
	})
}

func run2(v1 int) (err error) {
	db := sql.Db()
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}()
	having, err := dbutil.DbQueryI2(tx, "SELECT VALUE FROM TEST_VAL1 WHERE TID=? FOR UPDATE", v1)
	// having, err := dbutil.DbQueryI2(tx, "SELECT VALUE FROM TEST_VAL1 WHERE TID=?", v1)
	if err != nil {
		return
	}
	old, err := dbutil.DbQueryI2(tx, "SELECT VALUE FROM TEST_VAL2 WHERE TID=? FOR UPDATE", v1)
	// old, err := dbutil.DbQueryI2(tx, "SELECT VALUE FROM TEST_VAL2 WHERE TID=?", v1)
	if err != nil {
		return
	}
	if having > 10 {
		// fmt.Println("-->", having, old)
		_, err = tx.Exec("UPDATE TEST_VAL1 SET VALUE=? WHERE TID=?", having-10, v1)
		if err != nil {
			return
		}
		_, err = tx.Exec("UPDATE TEST_VAL2 SET VALUE=? WHERE TID=?", old+10, v1)
		if err != nil {
			return
		}
	}
	return
}

func test3() {
	perf := tutil.NewPerf()
	perf.ShowState = true
	sql.Db().Exec("UPDATE TEST_VAL1 SET VALUE=100")
	perf.AutoExec(1000, 100, 1, "", 100, func(idx int, state tutil.Perf) error {
		return nil
	}, func(v int) error {
		err := run3(v%3+1, (v+1)%3+1, (v+2)%3+1)
		if err != nil {
			panic(err)
		}
		return nil
	})
}

func run3(v1, v2, v3 int) (err error) {
	db := sql.Db()
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}()
	// _, err = tx.Exec("LOCK TABLES TEST_VAL1 WRITE")
	// if err != nil {
	// 	return
	// }
	_, err = tx.Query("SELECT * FROM TEST_VAL1 WHERE TID=? OR TID=? OR TID=?", v1, v2, v3)
	// _, err = dbutil.DbQueryInt2(tx, "SELECT VALUE FROM TEST_VAL1 WHERE TID=? OR TID=? OR TID=? FOR UPDATE", v1, v2, v3)
	if err != nil {
		return
	}
	_, err = tx.Exec("UPDATE TEST_VAL1 SET VALUE=VALUE-20 WHERE TID=?", v1)
	if err != nil {
		return
	}
	_, err = tx.Exec("UPDATE TEST_VAL1 SET VALUE=VALUE+10 WHERE TID=?", v2)
	if err != nil {
		return
	}
	_, err = tx.Exec("UPDATE TEST_VAL1 SET VALUE=VALUE+10 WHERE TID=?", v3)
	if err != nil {
		return
	}
	// _, err = tx.Exec("UNLOCK TABLES")
	// if err != nil {
	// 	return
	// }
	return
}
