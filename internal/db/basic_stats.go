package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"SnapFlow/internal/models"
)

// FillBasicStats 从statistics表获取基本流量统计并填充到snapshot中
func FillBasicStats(ctx context.Context, db *sql.DB, statsTableName string, snapshot *models.Snapshot) error {

	// 查询最新的统计记录
	query := fmt.Sprintf(`
		SELECT 
			packets_sum, 
			packet_size_sum, 
			time_window, 
			update_at
		FROM %s
		ORDER BY time_window DESC, update_at DESC
		LIMIT 1
	`, statsTableName)

	var packetsSum uint64
	var packetSizeSum uint64
	var timeWindow time.Time
	var updateAt time.Time

	err := db.QueryRowContext(ctx, query).Scan(
		&packetsSum,
		&packetSizeSum,
		&timeWindow,
		&updateAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			// 设置默认值
			snapshot.Basic.TotalPackets = 0
			snapshot.Basic.TotalBytes = 0
			snapshot.Basic.StartTime = time.Now().Add(-1 * time.Minute)
			snapshot.Basic.EndTime = time.Now()
			return nil
		}
		return fmt.Errorf("获取基本统计数据失败: %w", err)
	}

	// 填充snapshot的Basic字段
	snapshot.Basic.TotalPackets = packetsSum
	snapshot.Basic.TotalBytes = packetSizeSum
	snapshot.Basic.StartTime = timeWindow // 使用time_window作为开始时间
	snapshot.Basic.EndTime = updateAt     // 使用update_at作为结束时间

	return nil
}
