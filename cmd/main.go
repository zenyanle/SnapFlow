package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"SnapFlow/internal/db"
	"SnapFlow/internal/models"
)

func main() {

	// 设置上下文
	ctx := context.Background()

	// 连接到数据库（GrepTimeDB和MySQL共享同一个连接）
	database, err := connectToDatabase()
	if err != nil {
		log.Fatalf("连接到数据库失败: %v", err)
	}
	defer database.Close()

	// 创建所需的表
	if err := db.CreateGrepTimeDBTables(ctx, database); err != nil {
		log.Fatalf("创建GrepTimeDB表失败: %v", err)
	}
	fmt.Println("✓ GrepTimeDB表创建完成")

	// 设置定时器，每5秒执行一次
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// 设置信号处理以便于优雅退出
	done := make(chan bool)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("\n开始自动快照采集，每5秒一次。按Ctrl+C退出...\n")

	// 启动时立即执行一次
	go collectAndSaveSnapshot(ctx, database)

	// 主循环
	go func() {
		snapshotCount := 1
		for {
			select {
			case <-ticker.C:
				fmt.Printf("\n--- 开始采集第 %d 个快照 ---\n", snapshotCount)
				collectAndSaveSnapshot(ctx, database)
				snapshotCount++
			case <-done:
				return
			}
		}
	}()

	// 等待退出信号
	<-sigChan
	fmt.Println("\n接收到退出信号，正在关闭...")
	done <- true
	fmt.Println("程序已退出")
}

// collectAndSaveSnapshot 收集网络流量快照并保存到GrepTimeDB
func collectAndSaveSnapshot(ctx context.Context, database *sql.DB) {

	// 设置表名
	packetTableName := "packet_data"
	statsTableName := "packets_statistics2"

	// 创建新快照
	snapshot := models.NewSnapshot()

	fmt.Println("开始收集网络流量统计数据...")

	// 1. 填充基本统计信息
	if err := db.FillBasicStats(ctx, database, statsTableName, snapshot); err != nil {
		log.Printf("填充基本统计信息失败: %v", err)
	} else {
		fmt.Println("✓ 基本统计数据收集完成")
	}

	// 2. 填充IP统计
	if err := db.FillIPStats(ctx, database, packetTableName, snapshot); err != nil {
		log.Printf("填充IP统计失败: %v", err)
	} else {
		fmt.Println("✓ IP统计数据收集完成")
	}

	// 3. 填充端口统计
	if err := db.FillPortStats(ctx, database, packetTableName, snapshot); err != nil {
		log.Printf("填充端口统计失败: %v", err)
	} else {
		fmt.Println("✓ 端口统计数据收集完成")
	}

	// 4. 填充协议统计
	if err := db.FillProtocolStats(ctx, database, packetTableName, snapshot); err != nil {
		log.Printf("填充协议统计失败: %v", err)
	} else {
		fmt.Println("✓ 协议统计数据收集完成")
	}

	// 5. 填充TCP标志统计
	if err := db.FillTCPFlagsStats(ctx, database, packetTableName, snapshot); err != nil {
		log.Printf("填充TCP标志统计失败: %v", err)
	} else {
		fmt.Println("✓ TCP标志统计数据收集完成")
	}

	// 6. 将快照数据保存到GrepTimeDB
	fmt.Println("将网络流量快照保存到GrepTimeDB...")
	if err := db.SaveSnapshotToGrepTimeDB(ctx, database, snapshot); err != nil {
		log.Printf("保存快照到GrepTimeDB失败: %v", err)
		return
	}

	// 7. 显示统计摘要
	fmt.Printf("✓ 快照采集完成 - 总计 %d 个数据包，%d 字节\n",
		snapshot.Basic.TotalPackets,
		snapshot.Basic.TotalBytes)

	// 8. 可选：输出JSON格式的摘要
	if os.Getenv("VERBOSE_OUTPUT") == "true" {
		jsonStr, _ := snapshotToJSON(snapshot)
		fmt.Printf("快照摘要:\n%s\n", jsonStr)
	}
}

// connectToDatabase 连接到共享的数据库
func connectToDatabase() (*sql.DB, error) {
	// 获取数据库连接信息
	dbUser := getEnv("DB_USER", "greptime_user")
	dbPass := getEnv("DB_PASSWORD", "greptime_pwd")
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "4002")
	dbName := getEnv("DB_NAME", "test")

	// 构建连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		dbUser, dbPass, dbHost, dbPort, dbName)

	fmt.Println("正在连接到数据库...")

	// 连接数据库
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("数据库连接失败: %w", err)
	}

	// 设置连接池
	database.SetMaxOpenConns(10)
	database.SetMaxIdleConns(5)
	database.SetConnMaxLifetime(5 * time.Minute)

	// 检查数据库连接
	if err := database.Ping(); err != nil {
		database.Close()
		return nil, fmt.Errorf("无法连接到数据库: %w", err)
	}

	fmt.Println("✓ 成功连接到数据库")
	return database, nil
}

// getEnv 获取环境变量，如果不存在则返回默认值
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// snapshotToJSON 将快照序列化为美观的JSON字符串
func snapshotToJSON(snapshot *models.Snapshot) (string, error) {
	// 创建一个可读性更强的时间格式转换
	type jsonSnapshot struct {
		Timestamp string `json:"timestamp"`
		User      string `json:"user"`
		Basic     struct {
			TotalPackets uint64 `json:"total_packets"`
			TotalBytes   uint64 `json:"total_bytes"`
			StartTime    string `json:"start_time"`
			EndTime      string `json:"end_time"`
		} `json:"basic_stats"`
		IP struct {
			UniqueSourceCount int                    `json:"unique_source_count"`
			TopPairs          []models.IPAddressPair `json:"top_source_ips"`
		} `json:"ip_stats"`
		Port struct {
			UniqueDestCount int               `json:"unique_destination_count"`
			TopPairs        []models.PortPair `json:"top_destination_ports"`
		} `json:"port_stats"`
		Protocol struct {
			Protocols []models.ProtocolCount `json:"protocols"`
		} `json:"protocol_stats"`
		TCPFlags struct {
			Flags []models.TCPFlagCount `json:"flags"`
		} `json:"tcp_flags_stats"`
	}

	// 创建JSON结构
	jsonData := jsonSnapshot{
		Timestamp: snapshot.Timestamp.Format("2006-01-02 15:04:05"),
	}

	// 复制基本统计信息
	jsonData.Basic.TotalPackets = snapshot.Basic.TotalPackets
	jsonData.Basic.TotalBytes = snapshot.Basic.TotalBytes
	jsonData.Basic.StartTime = snapshot.Basic.StartTime.Format("2006-01-02 15:04:05")
	jsonData.Basic.EndTime = snapshot.Basic.EndTime.Format("2006-01-02 15:04:05")

	// 复制IP数据
	jsonData.IP.UniqueSourceCount = snapshot.IP.UniqueSourceCount
	for _, pair := range snapshot.IP.TopPairs {
		if pair.Count > 0 {
			jsonData.IP.TopPairs = append(jsonData.IP.TopPairs, pair)
		}
	}

	// 复制端口数据
	jsonData.Port.UniqueDestCount = snapshot.Port.UniqueDestCount
	for _, pair := range snapshot.Port.TopPairs {
		if pair.Count > 0 {
			jsonData.Port.TopPairs = append(jsonData.Port.TopPairs, pair)
		}
	}

	// 复制协议数据
	jsonData.Protocol.Protocols = snapshot.Protocol.Protocols

	// 复制TCP标志数据
	jsonData.TCPFlags.Flags = snapshot.TCPFlags.Flags

	// 序列化为带缩进的JSON
	jsonBytes, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return "", fmt.Errorf("序列化快照失败: %w", err)
	}

	return string(jsonBytes), nil
}
