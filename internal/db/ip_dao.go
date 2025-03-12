package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"SnapFlow/internal/models"
)

// FillIPStats 填充最近一分钟的IP统计数据到snapshot中
func FillIPStats(ctx context.Context, db *sql.DB, tableName string, snapshot *models.Snapshot) error {
	// 记录当前时间作为快照结束时间
	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Minute)

	// 更新快照的时间范围
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

	// 2. 获取前5个IP地址对
	topPairsQuery := fmt.Sprintf(`
		SELECT 
			IFNULL(src_ip, '') as src_ip, 
			IFNULL(dst_ip, '') as dst_ip, 
			COUNT(*) as count
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY src_ip, dst_ip
		ORDER BY count DESC
		LIMIT 5
	`, tableName)

	rows, err := db.QueryContext(ctx, topPairsQuery)
	if err != nil {
		return fmt.Errorf("获取前5个IP对失败: %w", err)
	}
	defer rows.Close()

	// 设置唯一源IP计数
	snapshot.IP.UniqueSourceCount = uniqueCount

	// 扫描IP对数据
	index := 0
	for rows.Next() && index < 5 {
		var srcIP, dstIP string
		var count uint64

		if err := rows.Scan(&srcIP, &dstIP, &count); err != nil {
			return fmt.Errorf("扫描IP对数据失败: %w", err)
		}

		snapshot.IP.TopPairs[index] = models.IPAddressPair{
			SourceIP:      srcIP,
			DestinationIP: dstIP,
			Count:         count,
		}

		index++
	}

	// 检查扫描错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("扫描数据时发生错误: %w", err)
	}

	return nil
}
