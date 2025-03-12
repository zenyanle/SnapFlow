package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func ConnectDatabase() *sql.DB {
	// 数据库连接信息 (替换成你的实际信息)
	dsn := "user:password@tcp(127.0.0.1:4002)/test?charset=utf8mb4&parseTime=True&loc=Local"

	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err) // 连接失败，记录错误并退出
	}

	// 测试连接
	err = db.Ping()
	if err != nil {
		log.Fatal(err) // 无法连接到数据库，记录错误并退出
	}

	fmt.Println("Successfully connected to MySQL database!")

	return db
}
