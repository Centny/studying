package main

import (
	"fmt"

	"github.com/Centny/gwf/util"
	"github.com/Centny/rediscache"
)

func main() {
	rediscache.InitRedisPool("loc.m:6379?db=1")
	rediscache.C().Send("SET", "A", "B") //for connect
	xx()
}

func xx() {
	args := []interface{}{}
	var total int64
	for k := 1000; k < 31000; k += 1000 {
		total = 0
		for i := 0; i < k; i++ {
			key, val := fmt.Sprintf("key-%v", i), fmt.Sprintf("val-%v", i)
			args = append(args, key, val)
			total += int64(len(key)) + int64(len(val))
		}
		beg := util.Now()
		err := rediscache.C().Send("MSET", args...)
		used := util.Now() - beg
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v->%v used %v\n", total/1024, k, used)
	}
	// rediscache.C().Send(commandName string, args ...interface{})
}

func imcase() {

}
