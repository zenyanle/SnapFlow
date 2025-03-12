package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"SnapFlow/internal/models"
)

// FillBasicStats 从统计表获取基本流量统计并填充到snapshot中
func FillBasicStats(ctx context.Context, db *sql.DB, statsTableName string, snapshot *models.Snapshot) error {
	// 查询最新的统计记录
	query := fmt.Sprintf(`
		SELECT 
			packets_sum, 
			packet_size_sum, 
			time_window, 
			update_at
		FROM %s
		ORDER BY time_window DESC
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

	// 计算时间窗口
	startTime := timeWindow
	endTime := updateAt

	// 填充snapshot的Basic字段
	snapshot.Basic.TotalPackets = packetsSum
	snapshot.Basic.TotalBytes = packetSizeSum
	snapshot.Basic.StartTime = startTime
	snapshot.Basic.EndTime = endTime

	return nil
}

// GetLastMinuteStats 获取过去一分钟的统计数据
func GetLastMinuteStats(ctx context.Context, db *sql.DB, statsTableName string) (uint64, uint64, error) {
	// 查询过去一分钟的统计记录总和
	query := fmt.Sprintf(`
		SELECT 
			SUM(packets_sum) as total_packets, 
			SUM(packet_size_sum) as total_bytes
		FROM %s
		WHERE time_window >= NOW() - INTERVAL 1 MINUTE
	`, statsTableName)

	var totalPackets, totalBytes sql.NullInt64

	err := db.QueryRowContext(ctx, query).Scan(&totalPackets, &totalBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("获取最近一分钟统计数据失败: %w", err)
	}

	// 处理NULL值情况
	var packetsCount, bytesCount uint64
	if totalPackets.Valid {
		packetsCount = uint64(totalPackets.Int64)
	}

	if totalBytes.Valid {
		bytesCount = uint64(totalBytes.Int64)
	}

	return packetsCount, bytesCount, nil
}
