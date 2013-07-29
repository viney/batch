package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
	"sync"
	"time"
)

var (
	db    *sql.DB
	mutex sync.Mutex
)

const (
	dsn = `host=192.168.1.138 port=4932 dbname=postgres user=postgres password=admin sslmode=disable`
)

func init() {
	mutex.Lock()
	defer mutex.Unlock()

	if db != nil {
		return
	}

	// open
	pdb, err := sql.Open("postgres", dsn)
	if err != nil {
		panic("sql.Open: " + err.Error())
	}

	// 删除表
	_, err = pdb.Exec(`drop table if exists tb_user`)
	if err != nil {
		log.Println("db.Exec: ", err.Error())
		return
	}

	// 创建表
	_, err = pdb.Exec(`create table if not exists tb_user(id serial primary key not null, name text, create_time timestamp(0) without time zone default current_timestamp)`)
	if err != nil {
		log.Println("pdb.Exec: ", err.Error())
		return
	}

	// 开启最大连接数
	pdb.SetMaxIdleConns(10000)

	db = pdb
}

func main() {
	// 开启事务
	var err error

	tx, err := db.Begin()
	if err != nil {
		log.Println("db.Begin: ", err.Error())
		return
	}

	defer func() {
		if err != nil && tx != nil {
			// 回滚
			if err := tx.Rollback(); err != nil {
				log.Println("tx.Rollback: ", err.Error())
				return
			}
		}
	}()

	// 编译sql语句
	// TODO:预编译一定要放在全局,防止sql语句重新编译
	stmt, err := tx.Prepare(`insert into tb_user(id, name, create_time) values($1, 'viney', default)`)
	if err != nil {
		log.Println("tx.Prepare: ", err.Error())
		return
	}
	defer func() {
		if err = stmt.Close(); err != nil {
			log.Println("stmt.Close: ", err.Error())
			return
		}
	}()

	finish := make(chan bool)
	count := 10000
	t := time.Now()
	for i := 0; i < count; i++ {
		go func(i int) {
			defer func() { finish <- true }()
			// 执行sql语句
			if _, err = stmt.Exec(i); err != nil {
				log.Println("stmt.Exec: ", err.Error())
				return
			}
		}(i)
	}

	for i := 0; i < count; i++ {
		<-finish
	}

	log.Println(time.Now().Sub(t))

	// 提交事务
	if err = tx.Commit(); err != nil {
		log.Println("tx.Commit: ", err.Error())
		return
	}

	// 关闭数据库连接
	if err = db.Close(); err != nil {
		log.Println("db.Close: ", err.Error())
		return
	}
}
