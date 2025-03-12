package db

import (
	"context"
	"database/sql"
	"fmt"

	"SnapFlow/internal/models"
)

// FillMACStats 填充MAC地址统计到snapshot中
func FillMACStats(ctx context.Context, db *sql.DB, tableName string, snapshot *models.Snapshot) error {
	// 1. 获取唯一源MAC地址数量
	uniqueCountQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT src_mac) 
		FROM %s 
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
	`, tableName)

	var uniqueCount int
	err := db.QueryRowContext(ctx, uniqueCountQuery).Scan(&uniqueCount)
	if err != nil {
		return fmt.Errorf("获取唯一MAC地址数量失败: %w", err)
	}

	// 2. 获取前5个出现频率最高的MAC地址
	topMACsQuery := fmt.Sprintf(`
		SELECT 
			IFNULL(src_mac, '') as src_mac, 
			COUNT(*) AS request_count
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY src_mac
		ORDER BY request_count DESC
		LIMIT 5
	`, tableName)

	rows, err := db.QueryContext(ctx, topMACsQuery)
	if err != nil {
		return fmt.Errorf("获取前5个MAC地址失败: %w", err)
	}
	defer rows.Close()

	// 设置唯一源MAC地址计数
	snapshot.MAC.UniqueSourceCount = uniqueCount

	// 扫描MAC地址数据
	index := 0
	for rows.Next() && index < 5 {
		var macAddress string
		var count uint64

		if err := rows.Scan(&macAddress, &count); err != nil {
			return fmt.Errorf("扫描MAC地址数据失败: %w", err)
		}

		snapshot.MAC.TopSources[index] = models.MACAddressCount{
			Address: macAddress,
			Count:   count,
		}

		index++
	}

	// 检查扫描错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("扫描MAC地址数据时发生错误: %w", err)
	}

	return nil
}
