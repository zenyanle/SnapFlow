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

		// 添加到结果集
		protocols = append(protocols, models.ProtocolCount{
			ID:         protocolID,
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

// GetProtocolDistribution 获取协议分布详情
func GetProtocolDistribution(ctx context.Context, db *sql.DB, tableName string) (map[string]float64, error) {
	query := fmt.Sprintf(`
		WITH total_packets AS (
			SELECT COUNT(*) AS total_count
			FROM %s
			WHERE ts >= NOW() - INTERVAL 1 MINUTE
		)
		SELECT 
			protocol AS protocol_id,
			(COUNT(*) * 100.0 / (SELECT total_count FROM total_packets)) AS percentage
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY protocol
		ORDER BY percentage DESC;
	`, tableName, tableName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("获取协议分布失败: %w", err)
	}
	defer rows.Close()

	distribution := make(map[string]float64)

	for rows.Next() {
		var protocolID int
		var percentage float64

		if err := rows.Scan(&protocolID, &percentage); err != nil {
			return nil, fmt.Errorf("扫描协议分布数据失败: %w", err)
		}

		protocolName := getProtocolName(protocolID)
		distribution[protocolName] = percentage
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return distribution, nil
}

// GetIPProtocolMap 返回IP协议号到名称的映射
func GetIPProtocolMap() map[int]string {
	return map[int]string{
		0:   "HOPOPT (IPv6 Hop-by-Hop Option)",
		1:   "ICMP (Internet Control Message)",
		2:   "IGMP (Internet Group Management)",
		3:   "GGP (Gateway-to-Gateway)",
		4:   "IPv4 (IPv4 encapsulation)",
		5:   "ST (Stream)",
		6:   "TCP (Transmission Control)",
		7:   "CBT (CBT)",
		8:   "EGP (Exterior Gateway Protocol)",
		9:   "IGP (any private interior gateway)",
		10:  "BBN-RCC-MON (BBN RCC Monitoring)",
		11:  "NVP-II (Network Voice Protocol)",
		12:  "PUP (PUP)",
		13:  "ARGUS (ARGUS)",
		14:  "EMCON (EMCON)",
		15:  "XNET (Cross Net Debugger)",
		16:  "CHAOS (Chaos)",
		17:  "UDP (User Datagram)",
		18:  "MUX (Multiplexing)",
		19:  "DCN-MEAS (DCN Measurement Subsystems)",
		20:  "HMP (Host Monitoring)",
		21:  "PRM (Packet Radio Measurement)",
		22:  "XNS-IDP (XEROX NS IDP)",
		23:  "TRUNK-1 (Trunk-1)",
		24:  "TRUNK-2 (Trunk-2)",
		25:  "LEAF-1 (Leaf-1)",
		26:  "LEAF-2 (Leaf-2)",
		27:  "RDP (Reliable Data Protocol)",
		28:  "IRTP (Internet Reliable Transaction)",
		29:  "ISO-TP4 (ISO Transport Protocol Class 4)",
		30:  "NETBLT (Bulk Data Transfer Protocol)",
		31:  "MFE-NSP (MFE Network Services Protocol)",
		32:  "MERIT-INP (MERIT Internodal Protocol)",
		33:  "DCCP (Datagram Congestion Control Protocol)",
		34:  "3PC (Third Party Connect Protocol)",
		35:  "IDPR (Inter-Domain Policy Routing Protocol)",
		36:  "XTP (XTP)",
		37:  "DDP (Datagram Delivery Protocol)",
		38:  "IDPR-CMTP (IDPR Control Message Transport Proto)",
		39:  "TP++ (TP++ Transport Protocol)",
		40:  "IL (IL Transport Protocol)",
		41:  "IPv6 (IPv6 encapsulation)",
		42:  "SDRP (Source Demand Routing Protocol)",
		43:  "IPv6-Route (Routing Header for IPv6)",
		44:  "IPv6-Frag (Fragment Header for IPv6)",
		45:  "IDRP (Inter-Domain Routing Protocol)",
		46:  "RSVP (Reservation Protocol)",
		47:  "GRE (Generic Routing Encapsulation)",
		48:  "DSR (Dynamic Source Routing Protocol)",
		49:  "BNA (BNA)",
		50:  "ESP (Encap Security Payload)",
		51:  "AH (Authentication Header)",
		52:  "I-NLSP (Integrated Net Layer Security TUBA)",
		53:  "SWIPE (IP with Encryption)",
		54:  "NARP (NBMA Address Resolution Protocol)",
		55:  "MOBILE (IP Mobility)",
		56:  "TLSP (Transport Layer Security Protocol)",
		57:  "SKIP (SKIP)",
		58:  "IPv6-ICMP (ICMP for IPv6)",
		59:  "IPv6-NoNxt (No Next Header for IPv6)",
		60:  "IPv6-Opts (Destination Options for IPv6)",
		61:  "any host internal protocol",
		62:  "CFTP (CFTP)",
		63:  "any local network",
		64:  "SAT-EXPAK (SATNET and Backroom EXPAK)",
		65:  "KRYPTOLAN (Kryptolan)",
		66:  "RVD (MIT Remote Virtual Disk Protocol)",
		67:  "IPPC (Internet Pluribus Packet Core)",
		68:  "any distributed file system",
		69:  "SAT-MON (SATNET Monitoring)",
		70:  "VISA (VISA Protocol)",
		71:  "IPCV (Internet Packet Core Utility)",
		72:  "CPNX (Computer Protocol Network Executive)",
		73:  "CPHB (Computer Protocol Heart Beat)",
		74:  "WSN (Wang Span Network)",
		75:  "PVP (Packet Video Protocol)",
		76:  "BR-SAT-MON (Backroom SATNET Monitoring)",
		77:  "SUN-ND (SUN ND PROTOCOL-Temporary)",
		78:  "WB-MON (WIDEBAND Monitoring)",
		79:  "WB-EXPAK (WIDEBAND EXPAK)",
		80:  "ISO-IP (ISO Internet Protocol)",
		81:  "VMTP (VMTP)",
		82:  "SECURE-VMTP (SECURE-VMTP)",
		83:  "VINES (VINES)",
		84:  "TTP (Transaction Transport Protocol)",
		85:  "NSFNET-IGP (NSFNET-IGP)",
		86:  "DGP (Dissimilar Gateway Protocol)",
		87:  "TCF (TCF)",
		88:  "EIGRP (EIGRP)",
		89:  "OSPFIGP (OSPFIGP)",
		90:  "Sprite-RPC (Sprite RPC Protocol)",
		91:  "LARP (Locus Address Resolution Protocol)",
		92:  "MTP (Multicast Transport Protocol)",
		93:  "AX.25 (AX.25 Frames)",
		94:  "IPIP (IP-within-IP Encapsulation Protocol)",
		95:  "MICP (Mobile Internetworking Control Pro.)",
		96:  "SCC-SP (Semaphore Communications Sec. Pro.)",
		97:  "ETHERIP (Ethernet-within-IP Encapsulation)",
		98:  "ENCAP (Encapsulation Header)",
		99:  "any private encryption scheme",
		100: "GMTP (GMTP)",
		101: "IFMP (Ipsilon Flow Management Protocol)",
		102: "PNNI (PNNI over IP)",
		103: "PIM (Protocol Independent Multicast)",
		104: "ARIS (ARIS)",
		105: "SCPS (SCPS)",
		106: "QNX (QNX)",
		107: "A/N (Active Networks)",
		108: "IPComp (IP Payload Compression Protocol)",
		109: "SNP (Sitara Networks Protocol)",
		110: "Compaq-Peer (Compaq Peer Protocol)",
		111: "IPX-in-IP (IPX in IP)",
		112: "VRRP (Virtual Router Redundancy Protocol)",
		113: "PGM (PGM Reliable Transport Protocol)",
		114: "any 0-hop protocol",
		115: "L2TP (Layer Two Tunneling Protocol)",
		116: "DDX (D-II Data Exchange (DDX))",
		117: "IATP (Interactive Agent Transfer Protocol)",
		118: "STP (Schedule Transfer Protocol)",
		119: "SRP (SpectraLink Radio Protocol)",
		120: "UTI (UTI)",
		121: "SMP (Simple Message Protocol)",
		122: "SM (Simple Multicast Protocol)",
		123: "PTP (Performance Transparency Protocol)",
		124: "ISIS over IPv4",
		125: "FIRE",
		126: "CRTP (Combat Radio Transport Protocol)",
		127: "CRUDP (Combat Radio User Datagram)",
		128: "SSCOPMCE",
		129: "IPLT",
		130: "SPS (Secure Packet Shield)",
		131: "PIPE (Private IP Encapsulation within IP)",
		132: "SCTP (Stream Control Transmission Protocol)",
		133: "FC (Fibre Channel)",
		134: "RSVP-E2E-IGNORE",
		135: "Mobility Header",
		136: "UDPLite",
		137: "MPLS-in-IP",
		138: "manet (MANET Protocols)",
		139: "HIP (Host Identity Protocol)",
		140: "Shim6 (Shim6 Protocol)",
		141: "WESP (Wrapped Encapsulating Security Payload)",
		142: "ROHC (Robust Header Compression)",
		143: "Ethernet",
		144: "AGGFRAG (AGGFRAG encapsulation)",
		145: "NSH (Network Service Header)",
	}
}
