package db

import (
	"context"
	"database/sql"
	"fmt"

	"SnapFlow/internal/models"
)

// FillPortStats 填充端口统计数据到snapshot中
func FillPortStats(ctx context.Context, db *sql.DB, tableName string, snapshot *models.Snapshot) error {
	// 打印固定的时间和用户信息
	fmt.Printf("Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): 2025-03-13 03:35:13\n")
	fmt.Printf("Current User's Login: zenyanle\n")

	// 1. 获取唯一目标端口数量
	uniqueCountQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT dst_port) 
		FROM %s 
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
	`, tableName)

	var uniqueCount int
	err := db.QueryRowContext(ctx, uniqueCountQuery).Scan(&uniqueCount)
	if err != nil {
		return fmt.Errorf("获取唯一目标端口数量失败: %w", err)
	}

	// 2. 获取前5个出现频率最高的目标端口
	topPortsQuery := fmt.Sprintf(`
		SELECT 
			dst_port, 
			COUNT(*) AS count
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY dst_port
		ORDER BY count DESC
		LIMIT 5
	`, tableName)

	rows, err := db.QueryContext(ctx, topPortsQuery)
	if err != nil {
		return fmt.Errorf("获取前5个目标端口失败: %w", err)
	}
	defer rows.Close()

	// 设置唯一目标端口计数
	snapshot.Port.UniqueDestCount = uniqueCount

	// 扫描端口数据
	index := 0
	for rows.Next() && index < 5 {
		var dstPort uint16
		var count uint64

		if err := rows.Scan(&dstPort, &count); err != nil {
			return fmt.Errorf("扫描端口数据失败: %w", err)
		}

		snapshot.Port.TopPairs[index] = models.PortPair{
			DestinationPort: dstPort,
			Count:           count,
		}

		index++
	}

	// 检查扫描错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("扫描端口数据时发生错误: %w", err)
	}

	// 打印获取的信息
	fmt.Printf("\n获取到的端口统计信息:\n")
	fmt.Printf("- 唯一目标端口数量: %d\n", uniqueCount)
	fmt.Printf("- 最活跃的目标端口 (前5个):\n")

	for i, port := range snapshot.Port.TopPairs {
		if port.Count > 0 {
			// 尝试识别常见端口的服务名称
			serviceName := identifyPortService(port.DestinationPort)
			fmt.Printf("  %d. 端口 %d (%s): %d 个数据包\n",
				i+1, port.DestinationPort, serviceName, port.Count)
		}
	}

	return nil
}

// identifyPortService 根据端口号识别常见服务
func identifyPortService(port uint16) string {
	portServiceMap := map[uint16]string{
		20:    "FTP-data",
		21:    "FTP",
		22:    "SSH",
		23:    "Telnet",
		25:    "SMTP",
		53:    "DNS",
		67:    "DHCP-Server",
		68:    "DHCP-Client",
		80:    "HTTP",
		110:   "POP3",
		119:   "NNTP",
		123:   "NTP",
		143:   "IMAP",
		161:   "SNMP",
		162:   "SNMP-Trap",
		389:   "LDAP",
		443:   "HTTPS",
		445:   "SMB",
		465:   "SMTPS",
		636:   "LDAPS",
		993:   "IMAPS",
		995:   "POP3S",
		1433:  "MSSQL",
		3306:  "MySQL",
		3389:  "RDP",
		5432:  "PostgreSQL",
		8080:  "HTTP-Alt",
		8443:  "HTTPS-Alt",
		27017: "MongoDB",
	}

	if service, exists := portServiceMap[port]; exists {
		return service
	}

	// 一些端口范围的通用规则
	if port < 1024 {
		return "系统/保留端口"
	} else if port >= 1024 && port <= 49151 {
		return "注册端口"
	} else {
		return "动态/私有端口"
	}
}
