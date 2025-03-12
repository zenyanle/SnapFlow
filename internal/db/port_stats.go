package db

import (
	"context"
	"database/sql"
	"fmt"

	"SnapFlow/internal/models"
)

// FillPortStats 填充端口统计数据到snapshot中
func FillPortStats(ctx context.Context, db *sql.DB, tableName string, snapshot *models.Snapshot) error {
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

	// 2. 获取前5个端口对
	topPortPairsQuery := fmt.Sprintf(`
		SELECT 
			src_port, 
			dst_port, 
			COUNT(*) AS count
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY src_port, dst_port
		ORDER BY count DESC
		LIMIT 5
	`, tableName)

	rows, err := db.QueryContext(ctx, topPortPairsQuery)
	if err != nil {
		return fmt.Errorf("获取前5个端口对失败: %w", err)
	}
	defer rows.Close()

	// 设置唯一目标端口计数
	snapshot.Port.UniqueDestCount = uniqueCount

	// 扫描端口对数据
	index := 0
	for rows.Next() && index < 5 {
		var srcPort, dstPort uint16
		var count uint64

		if err := rows.Scan(&srcPort, &dstPort, &count); err != nil {
			return fmt.Errorf("扫描端口对数据失败: %w", err)
		}

		snapshot.Port.TopPairs[index] = models.PortPair{
			SourcePort:      srcPort,
			DestinationPort: dstPort,
			Count:           count,
		}

		index++
	}

	// 检查扫描错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("扫描端口对数据时发生错误: %w", err)
	}

	return nil
}

// GetPortServiceMap 根据端口获取可能的服务名称
func GetPortServiceMap() map[uint16]string {
	return map[uint16]string{
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
		137:   "NetBIOS-NS",
		138:   "NetBIOS-DGM",
		139:   "NetBIOS-SSN",
		143:   "IMAP",
		161:   "SNMP",
		162:   "SNMP-Trap",
		389:   "LDAP",
		443:   "HTTPS",
		445:   "SMB",
		465:   "SMTPS",
		514:   "Syslog",
		554:   "RTSP",
		631:   "IPP",
		636:   "LDAPS",
		993:   "IMAPS",
		995:   "POP3S",
		1433:  "MSSQL",
		1434:  "MSSQL-Monitor",
		1521:  "Oracle",
		3306:  "MySQL",
		3389:  "RDP",
		5060:  "SIP",
		5061:  "SIPS",
		5432:  "PostgreSQL",
		5900:  "VNC",
		8080:  "HTTP-Alt",
		8443:  "HTTPS-Alt",
		27017: "MongoDB",
	}
}
