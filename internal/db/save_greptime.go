package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"SnapFlow/internal/models"
)

// CreateGrepTimeDBTables 在GrepTimeDB中创建所有必要的表
func CreateGrepTimeDBTables(ctx context.Context, db *sql.DB) error {

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

	// 8. 新增: TCP 标志 JSON 统计表（用于 Grafana 饼图）
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_tcp_flags_json (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			flags_json STRING,
			total_packet_count UINT64,
			PRIMARY KEY(snapshot_id)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_tcp_flags_json 表失败: %w", err)
	}

	// 9. 新增: 协议 JSON 统计表（用于 Grafana 饼图）
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS network_protocols_json (
			snapshot_id STRING,
			ts TIMESTAMP TIME INDEX,
			protocols_json STRING,
			total_packet_count UINT64,
			PRIMARY KEY(snapshot_id)
		) with('append_mode'='true');
	`); err != nil {
		return fmt.Errorf("创建 network_protocols_json 表失败: %w", err)
	}

	fmt.Println("所有GrepTimeDB数据表创建成功")
	return nil
}

// SaveSnapshotToGrepTimeDB 将快照数据保存到GrepTimeDB
func SaveSnapshotToGrepTimeDB(ctx context.Context, db *sql.DB, snapshot *models.Snapshot) error {

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

	// 6. 新增: 插入协议JSON统计数据（用于Grafana饼图）
	fmt.Println("插入协议JSON数据...")
	if err := saveProtocolsJSON(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入协议JSON统计数据失败: %w", err)
	}

	// 7. 新增: 插入TCP标志JSON统计数据（用于Grafana饼图）
	fmt.Println("插入TCP标志JSON数据...")
	if err := saveTCPFlagsJSON(ctx, db, snapshot, now, snapshotID); err != nil {
		return fmt.Errorf("插入TCP标志JSON统计数据失败: %w", err)
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

// 新增: saveProtocolsJSON 保存协议JSON统计数据
func saveProtocolsJSON(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	// 1. 构建JSON数据
	type ProtocolData struct {
		Name       string  `json:"name"`
		Count      uint64  `json:"count"`
		Percentage float64 `json:"percentage"`
	}

	type ProtocolsJSON struct {
		Protocols []ProtocolData `json:"protocols"`
	}

	// 2. 填充数据
	protocolsJSON := ProtocolsJSON{
		Protocols: make([]ProtocolData, 0, len(snapshot.Protocol.Protocols)),
	}

	// 计算总数据包数
	var totalCount uint64 = 0
	for _, proto := range snapshot.Protocol.Protocols {
		totalCount += proto.Count
	}

	// 填充JSON数据
	for _, proto := range snapshot.Protocol.Protocols {
		protocolsJSON.Protocols = append(protocolsJSON.Protocols, ProtocolData{
			Name:       proto.Name,
			Count:      proto.Count,
			Percentage: proto.Percentage,
		})
	}

	// 3. 序列化为JSON字符串
	jsonBytes, err := json.Marshal(protocolsJSON)
	if err != nil {
		return fmt.Errorf("序列化协议JSON数据失败: %w", err)
	}

	// 4. 保存到数据库
	query := `
		INSERT INTO network_protocols_json(
			snapshot_id, ts, protocols_json, total_packet_count
		) VALUES(?, ?, ?, ?)
	`

	_, err = db.ExecContext(ctx, query,
		snapshotID,
		ts,
		string(jsonBytes),
		totalCount,
	)

	if err != nil {
		return fmt.Errorf("保存协议JSON数据失败: %w", err)
	}

	fmt.Println("- 保存了协议JSON数据用于饼图展示")
	return nil
}

// 新增: saveTCPFlagsJSON 保存TCP标志JSON统计数据
func saveTCPFlagsJSON(ctx context.Context, db *sql.DB, snapshot *models.Snapshot, ts time.Time, snapshotID string) error {
	// 1. 构建JSON数据
	type FlagData struct {
		Name       string  `json:"name"`
		Flag       string  `json:"flag"`
		Count      uint64  `json:"count"`
		Percentage float64 `json:"percentage"`
	}

	type TCPFlagsJSON struct {
		Flags []FlagData `json:"flags"`
	}

	// 2. 填充数据
	flagsJSON := TCPFlagsJSON{
		Flags: make([]FlagData, 0, len(snapshot.TCPFlags.Flags)),
	}

	// 计算总数据包数
	var totalCount uint64 = 0
	for _, flag := range snapshot.TCPFlags.Flags {
		totalCount += flag.Count
	}

	// 计算百分比并添加到JSON结构
	for _, flag := range snapshot.TCPFlags.Flags {
		var percentage float64 = 0
		if totalCount > 0 {
			percentage = float64(flag.Count) * 100 / float64(totalCount)
		}

		flagsJSON.Flags = append(flagsJSON.Flags, FlagData{
			Name:       getTCPFlagName(flag.Flag),
			Flag:       flag.Flag,
			Count:      flag.Count,
			Percentage: percentage,
		})
	}

	// 3. 序列化为JSON字符串
	jsonBytes, err := json.Marshal(flagsJSON)
	if err != nil {
		return fmt.Errorf("序列化TCP标志JSON数据失败: %w", err)
	}

	// 4. 保存到数据库
	query := `
		INSERT INTO network_tcp_flags_json(
			snapshot_id, ts, flags_json, total_packet_count
		) VALUES(?, ?, ?, ?)
	`

	_, err = db.ExecContext(ctx, query,
		snapshotID,
		ts,
		string(jsonBytes),
		totalCount,
	)

	if err != nil {
		return fmt.Errorf("保存TCP标志JSON数据失败: %w", err)
	}

	fmt.Println("- 保存了TCP标志JSON数据用于饼图展示")
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
