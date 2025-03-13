package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"SnapFlow/internal/models"
)

// FillProtocolStats 填充协议统计到snapshot中
func FillProtocolStats(ctx context.Context, db *sql.DB, tableName string, snapshot *models.Snapshot) error {

	// 使用WITH语句计算协议分布和百分比
	query := fmt.Sprintf(`
		WITH total_packets AS (
			-- 计算总的数据包数量
			SELECT COUNT(*) AS total_count
			FROM %s
			WHERE ts >= NOW() - INTERVAL 1 MINUTE
		)
		SELECT 
			protocol AS name, 
			COUNT(*) AS count, 
			(COUNT(*) * 100.0 / (SELECT total_count FROM total_packets)) AS percentage
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY protocol
		ORDER BY count DESC;
	`, tableName, tableName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("获取协议统计失败: %w", err)
	}
	defer rows.Close()

	// 存储协议统计
	var protocols []models.ProtocolCount

	// 扫描协议数据
	for rows.Next() {
		var protocolID int
		var count uint64
		var percentage float64

		if err := rows.Scan(&protocolID, &count, &percentage); err != nil {
			return fmt.Errorf("扫描协议数据失败: %w", err)
		}

		// 将协议ID转换为可读名称
		protocolName := getProtocolName(protocolID)

		// 添加到结果集 - 移除ID字段，只使用Name
		protocols = append(protocols, models.ProtocolCount{
			Name:       protocolName,
			Count:      count,
			Percentage: percentage,
		})
	}

	// 检查扫描错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("扫描协议数据时发生错误: %w", err)
	}

	// 设置协议统计
	snapshot.Protocol.Protocols = protocols

	// 打印获取的信息
	fmt.Printf("\n获取到的协议统计信息:\n")
	fmt.Printf("- 协议统计数量: %d\n", len(protocols))

	// 打印前5个协议信息（如果有）
	fmt.Printf("- 主要协议:\n")
	maxToPrint := 5
	if len(protocols) < maxToPrint {
		maxToPrint = len(protocols)
	}

	for i := 0; i < maxToPrint; i++ {
		fmt.Printf("  %s: %.2f%% (%d 个数据包)\n",
			protocols[i].Name,
			protocols[i].Percentage,
			protocols[i].Count)
	}

	return nil
}

// getProtocolName 通过协议ID获取协议名称
func getProtocolName(protocolID int) string {
	protocolMap := map[int]string{
		0:   "HOPOPT",
		1:   "ICMP",
		2:   "IGMP",
		6:   "TCP",
		8:   "EGP",
		9:   "IGP",
		17:  "UDP",
		41:  "IPv6",
		43:  "IPv6-Route",
		44:  "IPv6-Frag",
		47:  "GRE",
		50:  "ESP",
		51:  "AH",
		58:  "IPv6-ICMP",
		88:  "EIGRP",
		89:  "OSPF",
		103: "PIM",
		112: "VRRP",
		115: "L2TP",
		132: "SCTP",
		136: "UDPLite",
	}

	if name, exists := protocolMap[protocolID]; exists {
		return name
	}

	// 未知协议返回ID字符串
	return "Protocol-" + strconv.Itoa(protocolID)
}
