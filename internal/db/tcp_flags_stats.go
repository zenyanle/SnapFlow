package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"SnapFlow/internal/models"
)

// FillTCPFlagsStats 填充TCP标志统计到snapshot中
func FillTCPFlagsStats(ctx context.Context, db *sql.DB, tableName string, snapshot *models.Snapshot) error {
	// 使用WITH语句计算TCP标志分布和百分比
	query := fmt.Sprintf(`
		WITH total_packets AS (
			-- 计算总的数据包数量
			SELECT COUNT(*) AS total_count
			FROM %s
			WHERE ts >= NOW() - INTERVAL 1 MINUTE
		)
		SELECT 
			tcp_flags AS name, 
			COUNT(*) AS count, 
			(COUNT(*) * 100.0 / (SELECT total_count FROM total_packets)) AS percentage
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY tcp_flags
		ORDER BY count DESC;
	`, tableName, tableName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("获取TCP标志统计失败: %w", err)
	}
	defer rows.Close()

	// 存储TCP标志统计
	var tcpFlags []models.TCPFlagCount

	// 扫描TCP标志数据
	for rows.Next() {
		var flagValue int
		var count uint64
		var percentage float64

		if err := rows.Scan(&flagValue, &count, &percentage); err != nil {
			return fmt.Errorf("扫描TCP标志数据失败: %w", err)
		}

		// 将数值转换为TCP标志文本描述
		flagName := tcpFlagToString(uint8(flagValue))

		// 添加到结果集
		tcpFlags = append(tcpFlags, models.TCPFlagCount{
			Flag:  flagName,
			Count: count,
		})
	}

	// 检查扫描错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("扫描TCP标志数据时发生错误: %w", err)
	}

	// 设置TCP标志统计
	snapshot.TCPFlags.Flags = tcpFlags

	return nil
}

// tcpFlagToString 将TCP标志值转换为字符串
func tcpFlagToString(flag uint8) string {
	return strconv.Itoa(int(flag))
}

// describeTCPFlag 描述TCP标志位组合
func describeTCPFlag(flag uint8) string {
	var description string

	// 检查各个位
	if flag&0x01 != 0 {
		description += "FIN "
	}
	if flag&0x02 != 0 {
		description += "SYN "
	}
	if flag&0x04 != 0 {
		description += "RST "
	}
	if flag&0x08 != 0 {
		description += "PSH "
	}
	if flag&0x10 != 0 {
		description += "ACK "
	}
	if flag&0x20 != 0 {
		description += "URG "
	}
	if flag&0x40 != 0 {
		description += "ECE "
	}
	if flag&0x80 != 0 {
		description += "CWR "
	}

	if description == "" {
		return "No flags"
	}

	return description
}

// GetCommonTCPFlagCombinations 返回常见TCP标志组合的说明
func GetCommonTCPFlagCombinations() map[uint8]string {
	return map[uint8]string{
		0x00: "No flags (unusual)",
		0x01: "FIN - Connection finish",
		0x02: "SYN - Connection start",
		0x03: "SYN+FIN (invalid combination)",
		0x04: "RST - Connection reset",
		0x08: "PSH - Push data",
		0x10: "ACK - Acknowledgment",
		0x12: "SYN+ACK - Connection establishment acknowledgment",
		0x18: "PSH+ACK - Pushing data with acknowledgment (common for data transfer)",
		0x11: "FIN+ACK - Connection termination with acknowledgment",
		0x14: "RST+ACK - Connection reset with acknowledgment",
		0x19: "FIN+PSH+ACK - Final data push with acknowledgment",
	}
}

// AnalyzeTCPFlagPatterns 分析TCP标志模式，检查连接建立和异常
func AnalyzeTCPFlagPatterns(ctx context.Context, db *sql.DB, tableName string) (map[string]int, error) {
	// 查询SYN次数
	synQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s 
		WHERE ts >= NOW() - INTERVAL 1 MINUTE 
		AND tcp_flags & 0x02 > 0
	`, tableName)

	// 查询SYN+ACK次数
	synAckQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s 
		WHERE ts >= NOW() - INTERVAL 1 MINUTE 
		AND tcp_flags & 0x12 = 0x12
	`, tableName)

	// 查询RST次数
	rstQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s 
		WHERE ts >= NOW() - INTERVAL 1 MINUTE 
		AND tcp_flags & 0x04 > 0
	`, tableName)

	// 查询FIN次数
	finQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s 
		WHERE ts >= NOW() - INTERVAL 1 MINUTE 
		AND tcp_flags & 0x01 > 0
	`, tableName)

	var synCount, synAckCount, rstCount, finCount int

	if err := db.QueryRowContext(ctx, synQuery).Scan(&synCount); err != nil {
		return nil, fmt.Errorf("查询SYN标志失败: %w", err)
	}

	if err := db.QueryRowContext(ctx, synAckQuery).Scan(&synAckCount); err != nil {
		return nil, fmt.Errorf("查询SYN+ACK标志失败: %w", err)
	}

	if err := db.QueryRowContext(ctx, rstQuery).Scan(&rstCount); err != nil {
		return nil, fmt.Errorf("查询RST标志失败: %w", err)
	}

	if err := db.QueryRowContext(ctx, finQuery).Scan(&finCount); err != nil {
		return nil, fmt.Errorf("查询FIN标志失败: %w", err)
	}

	// 返回统计结果
	return map[string]int{
		"SYN":     synCount,
		"SYN+ACK": synAckCount,
		"RST":     rstCount,
		"FIN":     finCount,
	}, nil
}
