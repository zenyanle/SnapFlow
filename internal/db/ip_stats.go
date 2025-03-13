package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"SnapFlow/internal/models"
)

// FillIPStats 填充最近一分钟的源IP统计数据到snapshot中
func FillIPStats(ctx context.Context, db *sql.DB, tableName string, snapshot *models.Snapshot) error {

	// 更新快照的时间范围
	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Minute)

	snapshot.Basic.StartTime = startTime
	snapshot.Basic.EndTime = endTime

	// 1. 获取唯一源IP数量
	uniqueCountQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT src_ip) 
		FROM %s 
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
	`, tableName)

	var uniqueCount int
	err := db.QueryRowContext(ctx, uniqueCountQuery).Scan(&uniqueCount)
	if err != nil {
		return fmt.Errorf("获取唯一IP数量失败: %w", err)
	}

	// 2. 获取前5个出现频率最高的源IP地址
	topSourcesQuery := fmt.Sprintf(`
		SELECT 
			IFNULL(src_ip, '') as src_ip, 
			COUNT(*) as count
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY src_ip
		ORDER BY count DESC
		LIMIT 5
	`, tableName)

	rows, err := db.QueryContext(ctx, topSourcesQuery)
	if err != nil {
		return fmt.Errorf("获取前5个源IP地址失败: %w", err)
	}
	defer rows.Close()

	// 设置唯一源IP计数
	snapshot.IP.UniqueSourceCount = uniqueCount

	// 扫描源IP数据
	index := 0
	for rows.Next() && index < 5 {
		var srcIP string
		var count uint64

		if err := rows.Scan(&srcIP, &count); err != nil {
			return fmt.Errorf("扫描源IP数据失败: %w", err)
		}

		snapshot.IP.TopPairs[index] = models.IPAddressPair{
			SourceIP: srcIP,
			Count:    count,
		}

		index++
	}

	// 检查扫描错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("扫描数据时发生错误: %w", err)
	}

	// 打印获取的信息
	fmt.Printf("\n获取到的源IP统计信息:\n")
	fmt.Printf("- 唯一源IP地址数量: %d\n", uniqueCount)
	fmt.Printf("- 最活跃的源IP地址 (前5个):\n")

	for i, ip := range snapshot.IP.TopPairs {
		if ip.Count > 0 {
			fmt.Printf("  %d. %s: %d 个数据包\n", i+1, ip.SourceIP, ip.Count)
		}
	}

	return nil
}
