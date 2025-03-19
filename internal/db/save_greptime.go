package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"SnapFlow/internal/models"
)

// CreateGrepTimeDBTables 在GrepTimeDB中创建所有必要的表
func CreateGrepTimeDBTables(ctx context.Context, db *sql.DB) error {
	// 打印固定的时间和用户信息
	fmt.Printf("Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): 2025-03-19 10:02:56\n")
	fmt.Printf("Current User's Login: zenyanle\n")

	// 1. 基础统计表 - 使用snapshot_id作为主键
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_basic_stats (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			total_packets UINT64,
			total_bytes UINT64,
			window_start TIMESTAMP,
			window_end TIMESTAMP,
			window_size_seconds UINT16,
			PRIMARY KEY(snapshot_id)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_basic_stats 表失败: %w", err)
	}

	// 2. IP 统计表
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_ip_stats (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			unique_source_count UINT32,
			PRIMARY KEY(snapshot_id)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_ip_stats 表失败: %w", err)
	}

	// 3. 热门源 IP 表 - 使用pos_rank代替position
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_top_source_ips (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			source_ip STRING,
			pos_rank UINT8,
			packet_count UINT64,
			PRIMARY KEY(snapshot_id, pos_rank)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_top_source_ips 表失败: %w", err)
	}

	// 4. 端口统计表
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_port_stats (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			unique_dest_count UINT32,
			PRIMARY KEY(snapshot_id)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_port_stats 表失败: %w", err)
	}

	// 5. 热门目标端口表 - 使用pos_rank代替position
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_top_destination_ports (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			port UINT16,
			service_name STRING,
			pos_rank UINT8,
			packet_count UINT64,
			PRIMARY KEY(snapshot_id, pos_rank)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_top_destination_ports 表失败: %w", err)
	}

	// 6. 协议统计表
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_protocol_stats (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			protocol_name STRING,
			packet_count UINT64,
			percentage DOUBLE,
			PRIMARY KEY(snapshot_id, protocol_name)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_protocol_stats 表失败: %w", err)
	}

	// 7. TCP 标志统计表
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_tcp_flag_stats (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			flag STRING,
			flag_name STRING,
			packet_count UINT64,
			PRIMARY KEY(snapshot_id, flag)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_tcp_flag_stats 表失败: %w", err)
	}

	// 8. TCP 标志扁平化统计表（饼图用）
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_tcp_flags_json (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			total_packet_count UINT64,
			ack_count UINT64,
			syn_count UINT64, 
			fin_ack_count UINT64,
			psh_ack_count UINT64,
			none_count UINT64,
			other_count UINT64,
			PRIMARY KEY(snapshot_id)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_tcp_flags_json 表失败: %w", err)
	}

	// 9. 协议扁平化统计表（饼图用）
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_protocols_json (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			total_packet_count UINT64,
			tcp_count UINT64,
			udp_count UINT64,
			icmp_count UINT64,
			other_count UINT64,
			PRIMARY KEY(snapshot_id)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_protocols_json 表失败: %w", err)
	}

	// 10. 新增: 服务名称扁平化统计表（饼图用）
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_services_json (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			total_packet_count UINT64,
			http_count UINT64,
			https_count UINT64,
			ssh_count UINT64,
			dns_count UINT64,
			smtp_count UINT64,
			ftp_count UINT64,
			mysql_count UINT64,
			ntp_count UINT64,
			telnet_count UINT64,
			rdp_count UINT64,
			unknown_count UINT64,
			other_count UINT64,
			PRIMARY KEY(snapshot_id)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_services_json 表失败: %w", err)
	}

	fmt.Println("所有GrepTimeDB数据表创建成功")
	return nil
}

// SaveSnapshotToGrepTimeDB 将快照数据保存到GrepTimeDB
func SaveSnapshotToGrepTimeDB(ctx context.Context, db *sql.DB, snapshot *models.Snapshot) error {
	// 打印固定的时间和用户信息
	fmt.Printf("Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): 2025-03-19 10:02:56\n")
	fmt.Printf("Current User's Login: zenyanle\n")

	// 获取当前时间作为插入时间
	now := time.Now().UTC()

	// 生成快照ID
	snapshotID := fmt.Sprintf("snap_%d", now.UnixNano())

	// 1. 插入基础统计数据
	fmt.Println("插入基础统计数据...")
	if err := saveBasicStats(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入基础统计数据失败: %w", err)
	}

	// 2. 插入IP统计数据
	fmt.Println("插入IP统计数据...")
	if err := saveIPStats(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入IP统计数据失败: %w", err)
	}

	// 3. 插入端口统计数据
	fmt.Println("插入端口统计数据...")
	if err := savePortStats(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入端口统计数据失败: %w", err)
	}

	// 4. 插入协议统计数据
	fmt.Println("插入协议统计数据...")
	if err := saveProtocolStats(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入协议统计数据失败: %w", err)
	}

	// 5. 插入TCP标志统计数据
	fmt.Println("插入TCP标志统计数据...")
	if err := saveTCPFlagsStats(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入TCP标志统计数据失败: %w", err)
	}

	// 6. 插入协议扁平化统计数据
	fmt.Println("插入协议分布数据...")
	if err := saveProtocolsJSON(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入协议分布统计数据失败: %w", err)
	}

	// 7. 插入TCP标志扁平化统计数据
	fmt.Println("插入TCP标志分布数据...")
	if err := saveTCPFlagsJSON(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入TCP标志分布统计数据失败: %w", err)
	}

	// 8. 新增: 插入服务名称扁平化统计数据
	fmt.Println("插入服务名称分布数据...")
	if err := saveServicesJSON(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入服务名称分布统计数据失败: %w", err)
	}

	fmt.Println("网络流量快照已成功保存到GrepTimeDB")
	return nil
}

// saveBasicStats 插入基础统计数据
func saveBasicStats(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	query := `
		INSERT INTO network_basic_stats(
			snapshot_id, ts, total_packets, total_bytes, window_start, window_end, window_size_seconds
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`

	// 计算时间窗口大小（秒）
	windowSize := uint16(snapshot.Basic.EndTime.Sub(snapshot.Basic.StartTime).Seconds())

	_, err := db.ExecContext(ctx, query,
		snapshotID,
		ts,
		snapshot.Basic.TotalPackets,
		snapshot.Basic.TotalBytes,
		snapshot.Basic.StartTime,
		snapshot.Basic.EndTime,
		windowSize,
	)

	if err == nil {
		fmt.Printf("- 保存了 %d 个数据包、%d 字节的基础统计数据\n",
			snapshot.Basic.TotalPackets,
			snapshot.Basic.TotalBytes)
	}

	return err
}

// saveIPStats 插入IP统计数据
func saveIPStats(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	// 1. 插入IP统计摘要
	query1 := `
		INSERT INTO network_ip_stats(
			snapshot_id, ts, unique_source_count
		) VALUES(?, ?, ?)
	`

	if _, err := db.ExecContext(ctx, query1,
		snapshotID,
		ts,
		snapshot.IP.UniqueSourceCount,
	); err != nil {
		return err
	}

	// 2. 插入热门源IP
	query2 := `
		INSERT INTO network_top_source_ips(
			snapshot_id, ts, source_ip, pos_rank, packet_count
		) VALUES(?, ?, ?, ?, ?)
	`

	insertCount := 0
	for i, pair := range snapshot.IP.TopPairs {
		if pair.Count == 0 {
			continue // 跳过空记录
		}

		// 处理nil值
		sourceIP := pair.SourceIP
		if sourceIP == "<nil>" {
			sourceIP = "unknown"
		}

		if _, err := db.ExecContext(ctx, query2,
			snapshotID,
			ts,
			sourceIP,
			uint8(i+1), // 排名从1开始，转换为UINT8
			pair.Count,
		); err != nil {
			return err
		}

		insertCount++
	}

	fmt.Printf("- 保存了 %d 个唯一源IP和 %d 个热门源IP地址\n",
		snapshot.IP.UniqueSourceCount,
		insertCount)

	return nil
}

// savePortStats 插入端口统计数据
func savePortStats(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	// 1. 插入端口统计摘要
	query1 := `
		INSERT INTO network_port_stats(
			snapshot_id, ts, unique_dest_count
		) VALUES(?, ?, ?)
	`

	if _, err := db.ExecContext(ctx, query1,
		snapshotID,
		ts,
		snapshot.Port.UniqueDestCount,
	); err != nil {
		return err
	}

	// 2. 插入热门目标端口
	query2 := `
		INSERT INTO network_top_destination_ports(
			snapshot_id, ts, port, service_name, pos_rank, packet_count
		) VALUES(?, ?, ?, ?, ?, ?)
	`

	insertCount := 0
	for i, pair := range snapshot.Port.TopPairs {
		if pair.Count == 0 {
			continue // 跳过空记录
		}

		// 获取服务名称
		serviceName := getServiceNameByPort(pair.DestinationPort)

		if _, err := db.ExecContext(ctx, query2,
			snapshotID,
			ts,
			pair.DestinationPort,
			serviceName,
			uint8(i+1), // 排名从1开始，转换为UINT8
			pair.Count,
		); err != nil {
			return err
		}

		insertCount++
	}

	fmt.Printf("- 保存了 %d 个唯一目标端口和 %d 个热门目标端口\n",
		snapshot.Port.UniqueDestCount,
		insertCount)

	return nil
}

// saveProtocolStats 插入协议统计数据
func saveProtocolStats(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	query := `
		INSERT INTO network_protocol_stats(
			snapshot_id, ts, protocol_name, packet_count, percentage
		) VALUES(?, ?, ?, ?, ?)
	`

	insertCount := 0
	for _, proto := range snapshot.Protocol.Protocols {
		if _, err := db.ExecContext(ctx, query,
			snapshotID,
			ts,
			proto.Name,
			proto.Count,
			proto.Percentage,
		); err != nil {
			return err
		}

		insertCount++
	}

	fmt.Printf("- 保存了 %d 个协议统计记录\n", insertCount)

	return nil
}

// saveTCPFlagsStats 插入TCP标志统计数据
func saveTCPFlagsStats(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	query := `
		INSERT INTO network_tcp_flag_stats(
			snapshot_id, ts, flag, flag_name, packet_count
		) VALUES(?, ?, ?, ?, ?)
	`

	insertCount := 0
	for _, flag := range snapshot.TCPFlags.Flags {
		// 获取标志名称
		flagName := getTCPFlagName(flag.Flag)

		if _, err := db.ExecContext(ctx, query,
			snapshotID,
			ts,
			flag.Flag,
			flagName,
			flag.Count,
		); err != nil {
			return err
		}

		insertCount++
	}

	fmt.Printf("- 保存了 %d 个TCP标志统计记录\n", insertCount)

	return nil
}

// saveProtocolsJSON 保存协议分布统计数据（扁平化列结构）
func saveProtocolsJSON(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	// 计算总数据包数和各协议数量
	var totalCount, tcpCount, udpCount, icmpCount, otherCount uint64

	for _, proto := range snapshot.Protocol.Protocols {
		totalCount += proto.Count

		switch proto.Name {
		case "TCP":
			tcpCount = proto.Count
		case "UDP":
			udpCount = proto.Count
		case "ICMP":
			icmpCount = proto.Count
		default:
			otherCount += proto.Count
		}
	}

	// 构建并执行插入语句
	query := `
		INSERT INTO network_protocols_json(
			snapshot_id, ts, total_packet_count, tcp_count, udp_count, icmp_count, other_count
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`

	_, err := db.ExecContext(ctx, query,
		snapshotID,
		ts,
		totalCount,
		tcpCount,
		udpCount,
		icmpCount,
		otherCount,
	)

	if err != nil {
		return fmt.Errorf("保存协议分布数据失败: %w", err)
	}

	fmt.Println("- 保存了协议分布数据用于饼图展示")
	return nil
}

// saveTCPFlagsJSON 保存TCP标志分布统计数据（扁平化列结构）
func saveTCPFlagsJSON(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	// 计算总数据包数和各TCP标志数量
	var totalCount, ackCount, synCount, finAckCount, pshAckCount, noneCount, otherCount uint64

	for _, flag := range snapshot.TCPFlags.Flags {
		totalCount += flag.Count

		switch flag.Flag {
		case "16": // ACK
			ackCount = flag.Count
		case "2": // SYN
			synCount = flag.Count
		case "17": // FIN+ACK
			finAckCount = flag.Count
		case "24": // PSH+ACK
			pshAckCount = flag.Count
		case "0": // None
			noneCount = flag.Count
		default:
			otherCount += flag.Count
		}
	}

	// 构建并执行插入语句
	query := `
		INSERT INTO network_tcp_flags_json(
			snapshot_id, ts, total_packet_count, ack_count, syn_count, fin_ack_count, psh_ack_count, none_count, other_count
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := db.ExecContext(ctx, query,
		snapshotID,
		ts,
		totalCount,
		ackCount,
		synCount,
		finAckCount,
		pshAckCount,
		noneCount,
		otherCount,
	)

	if err != nil {
		return fmt.Errorf("保存TCP标志分布数据失败: %w", err)
	}

	fmt.Println("- 保存了TCP标志分布数据用于饼图展示")
	return nil
}

// 新增: saveServicesJSON 保存服务名称分布统计数据（扁平化列结构）
func saveServicesJSON(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	// 初始化计数器
	var (
		totalCount   uint64
		httpCount    uint64
		httpsCount   uint64
		sshCount     uint64
		dnsCount     uint64
		smtpCount    uint64
		ftpCount     uint64
		mysqlCount   uint64
		ntpCount     uint64
		telnetCount  uint64
		rdpCount     uint64
		unknownCount uint64
		otherCount   uint64
	)

	// 遍历所有热门端口记录，按服务名计数
	for _, pair := range snapshot.Port.TopPairs {
		if pair.Count == 0 {
			continue
		}

		totalCount += pair.Count

		// 根据端口号分类到相应的服务计数器
		switch pair.DestinationPort {
		case 80: // HTTP
			httpCount += pair.Count
		case 8080: // HTTP Alternate
			httpCount += pair.Count
		case 443, 8443: // HTTPS
			httpsCount += pair.Count
		case 22: // SSH
			sshCount += pair.Count
		case 53: // DNS
			dnsCount += pair.Count
		case 25: // SMTP
			smtpCount += pair.Count
		case 21: // FTP
			ftpCount += pair.Count
		case 3306: // MySQL
			mysqlCount += pair.Count
		case 123: // NTP
			ntpCount += pair.Count
		case 23: // Telnet
			telnetCount += pair.Count
		case 3389: // RDP
			rdpCount += pair.Count
		default:
			// 检查是否是高端口（可能是未知服务）
			if pair.DestinationPort >= 49152 {
				unknownCount += pair.Count
			} else {
				otherCount += pair.Count
			}
		}
	}

	// 构建并执行插入语句
	query := `
		INSERT INTO network_services_json(
			snapshot_id, ts, total_packet_count, 
			http_count, https_count, ssh_count, dns_count, smtp_count,
			ftp_count, mysql_count, ntp_count, telnet_count, rdp_count,
			unknown_count, other_count
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := db.ExecContext(ctx, query,
		snapshotID,
		ts,
		totalCount,
		httpCount,
		httpsCount,
		sshCount,
		dnsCount,
		smtpCount,
		ftpCount,
		mysqlCount,
		ntpCount,
		telnetCount,
		rdpCount,
		unknownCount,
		otherCount,
	)

	if err != nil {
		return fmt.Errorf("保存服务名称分布数据失败: %w", err)
	}

	fmt.Println("- 保存了服务名称分布数据用于饼图展示")
	return nil
}

// getServiceNameByPort 根据端口号获取服务名称
func getServiceNameByPort(port uint16) string {
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
		5353:  "mDNS",
		5432:  "PostgreSQL",
		8080:  "HTTP-Alt",
		8443:  "HTTPS-Alt",
		27017: "MongoDB",
	}

	if service, exists := portServiceMap[port]; exists {
		return service
	}

	if port < 1024 {
		return "系统端口"
	} else if port >= 1024 && port <= 49151 {
		return "注册端口"
	} else {
		return "动态端口"
	}
}

// getTCPFlagName 根据TCP标志值获取可读名称
func getTCPFlagName(flag string) string {
	flagNames := map[string]string{
		"0":  "None",
		"1":  "FIN",
		"2":  "SYN",
		"3":  "SYN+FIN",
		"4":  "RST",
		"8":  "PSH",
		"16": "ACK",
		"17": "FIN+ACK",
		"18": "SYN+ACK",
		"24": "PSH+ACK",
		"25": "FIN+PSH+ACK",
		"32": "URG",
		"48": "URG+ACK",
	}

	if name, exists := flagNames[flag]; exists {
		return name
	}

	return "复合标志位"
}
