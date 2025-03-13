package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"SnapFlow/internal/db"
	"SnapFlow/internal/models"
)

func main() {
	// 1. 打印固定的时间和用户信息已在各个填充函数中处理

	// 2. 获取数据库连接信息
	dbUser := getEnv("DB_USER", "greptime_user")
	dbPass := getEnv("DB_PASSWORD", "greptime_pwd")
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "4002")
	dbName := getEnv("DB_NAME", "test")

	// 3. 构建数据库连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPass, dbHost, dbPort, dbName)

	// 4. 连接数据库
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer database.Close()

	// 5. 检查数据库连接
	if err := database.Ping(); err != nil {
		log.Fatalf("无法连接到数据库: %v", err)
	}
	fmt.Println("已成功连接到数据库")

	// 6. 设置上下文和表名
	ctx := context.Background()
	packetTableName := "packet_data"
	statsTableName := "packets_statistics2"

	// 7. 创建新快照
	snapshot := models.NewSnapshot()

	// 8. 填充各种统计数据
	fmt.Println("开始收集网络流量统计数据...")

	// 首先填充基本统计信息
	if err := db.FillBasicStats(ctx, database, statsTableName, snapshot); err != nil {
		log.Printf("填充基本统计信息失败: %v", err)
	} else {
		fmt.Println("✓ 基本统计数据收集完成")
	}

	// 填充IP统计
	if err := db.FillIPStats(ctx, database, packetTableName, snapshot); err != nil {
		log.Printf("填充IP统计失败: %v", err)
	} else {
		fmt.Println("✓ IP统计数据收集完成")
	}

	// 填充端口统计
	if err := db.FillPortStats(ctx, database, packetTableName, snapshot); err != nil {
		log.Printf("填充端口统计失败: %v", err)
	} else {
		fmt.Println("✓ 端口统计数据收集完成")
	}

	// 填充协议统计
	if err := db.FillProtocolStats(ctx, database, packetTableName, snapshot); err != nil {
		log.Printf("填充协议统计失败: %v", err)
	} else {
		fmt.Println("✓ 协议统计数据收集完成")
	}

	// 填充TCP标志统计
	if err := db.FillTCPFlagsStats(ctx, database, packetTableName, snapshot); err != nil {
		log.Printf("填充TCP标志统计失败: %v", err)
	} else {
		fmt.Println("✓ TCP标志统计数据收集完成")
	}

	// 9. 设置快照的时间戳和用户信息
	parsedTime, _ := time.Parse("2006-01-02 15:04:05", fixedDateTimeUTC)
	snapshot.Timestamp = parsedTime
	snapshot.User = fixedUserLogin

	// 10. 将快照序列化为JSON并打印
	fmt.Println("\n网络流量快照数据 (JSON格式):")
	fmt.Println("=================================")

	jsonData, err := snapshotToJSON(snapshot)
	if err != nil {
		log.Fatalf("快照序列化失败: %v", err)
	}

	fmt.Println(jsonData)
	fmt.Println("=================================")
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
		User:      snapshot.User,
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
