package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// Snapshot 表示网络流量快照的主结构体
type Snapshot struct {
	Timestamp   time.Time        // 快照创建时间
	Basic       BasicStats       // 基本流量统计
	MAC         MACStats         // MAC地址统计
	IP          IPStats          // IP地址统计
	Port        PortStats        // 端口统计
	Protocol    ProtocolStats    // 协议统计
	TCPFlags    TCPFlagsStats    // TCP标志统计
	Application ApplicationStats // 应用层协议统计
}

// BasicStats 基本流量统计快照
type BasicStats struct {
	StartTime    time.Time // 时间窗口开始时间
	EndTime      time.Time // 时间窗口结束时间
	TotalPackets uint64    // 总数据包数
	TotalBytes   uint64    // 总字节数
}

// MACStats MAC地址统计
type MACStats struct {
	UniqueSourceCount int                // 唯一源MAC地址数量
	TopSources        [5]MACAddressCount // 最常见的源MAC地址(Top 5)
}

// MACAddressCount MAC地址及其出现次数
type MACAddressCount struct {
	Address string // MAC地址
	Count   uint64 // 出现次数
}

// IPStats IP地址统计
type IPStats struct {
	UniqueSourceCount int              // 唯一源IP地址数量
	TopPairs          [5]IPAddressPair // 出现频率最高的源IP地址对(Top 5)
}

// IPAddressPair IP地址对及其出现次数
type IPAddressPair struct {
	SourceIP string // 源IP地址
	Count    uint64 // 出现次数
}

// PortStats 端口统计
type PortStats struct {
	UniqueDestCount int         // 唯一目标端口数量
	TopPairs        [5]PortPair // 出现频率最高的端口对(Top 5)
}

// PortPair 端口对及其出现次数
type PortPair struct {
	DestinationPort uint16 // 目标端口
	Count           uint64 // 出现次数
}

// ProtocolStats 协议统计
type ProtocolStats struct {
	Protocols []ProtocolCount // 各协议统计
}

// ProtocolCount 协议及其统计信息
type ProtocolCount struct {
	Name       string  // 协议名称
	Count      uint64  // 数据包数量
	Percentage float64 // 占比(百分比)
}

// TCPFlagsStats TCP标志统计
type TCPFlagsStats struct {
	Flags []TCPFlagCount // 各TCP标志统计
}

// TCPFlagCount TCP标志及其统计信息
type TCPFlagCount struct {
	Flag  string // 标志名称(SYN, ACK, FIN, PSH, RST等)
	Count uint64 // 出现次数
}

// ApplicationStats 应用层协议统计
type ApplicationStats struct {
	Apps []ApplicationCount // 各应用统计
}

// ApplicationCount 应用及其统计信息
type ApplicationCount struct {
	Name       string  // 应用名称
	Count      uint64  // 数据包数量
	Percentage float64 // 占比(百分比)
}

// NewSnapshot 创建一个新的快照实例
func NewSnapshot() *Snapshot {
	return &Snapshot{
		Timestamp: time.Now().UTC(),
	}
}

// SetBasicStats 设置基本流量统计
func (s *Snapshot) SetBasicStats(startTime, endTime time.Time, totalPackets, totalBytes uint64) {
	s.Basic = BasicStats{
		StartTime:    startTime,
		EndTime:      endTime,
		TotalPackets: totalPackets,
		TotalBytes:   totalBytes,
	}
}

// SetMACStats 设置MAC地址统计
func (s *Snapshot) SetMACStats(uniqueCount int, topSources []MACAddressCount) {
	s.MAC = MACStats{
		UniqueSourceCount: uniqueCount,
	}

	// 填充前5项，不足的用空值填充
	for i := 0; i < 5; i++ {
		if i < len(topSources) {
			s.MAC.TopSources[i] = topSources[i]
		} else {
			// 使用空值占位符
			s.MAC.TopSources[i] = MACAddressCount{
				Address: "", // 空MAC地址作为占位符
				Count:   0,
			}
		}
	}
}

// SetIPStats 设置IP地址统计
func (s *Snapshot) SetIPStats(uniqueCount int, topPairs []IPAddressPair) {
	s.IP = IPStats{
		UniqueSourceCount: uniqueCount,
	}

	// 填充前5项，不足的用空值填充
	for i := 0; i < 5; i++ {
		if i < len(topPairs) {
			s.IP.TopPairs[i] = topPairs[i]
		} else {
			// 使用空值占位符
			s.IP.TopPairs[i] = IPAddressPair{
				SourceIP: "",
				Count:    0,
			}
		}
	}
}

// SetPortStats 设置端口统计
func (s *Snapshot) SetPortStats(uniqueCount int, topPairs []PortPair) {
	s.Port = PortStats{
		UniqueDestCount: uniqueCount,
	}

	// 填充前5项，不足的用空值填充
	for i := 0; i < 5; i++ {
		if i < len(topPairs) {
			s.Port.TopPairs[i] = topPairs[i]
		} else {
			// 使用空值占位符
			s.Port.TopPairs[i] = PortPair{
				DestinationPort: 0,
				Count:           0,
			}
		}
	}
}

// SetProtocolStats 设置协议统计
func (s *Snapshot) SetProtocolStats(protocols []ProtocolCount) {
	s.Protocol = ProtocolStats{
		Protocols: protocols,
	}
}

// SetTCPFlagsStats 设置TCP标志统计
func (s *Snapshot) SetTCPFlagsStats(flags []TCPFlagCount) {
	s.TCPFlags = TCPFlagsStats{
		Flags: flags,
	}
}

// SetApplicationStats 设置应用层协议统计
func (s *Snapshot) SetApplicationStats(apps []ApplicationCount) {
	s.Application = ApplicationStats{
		Apps: apps,
	}
}

// ToJSON 将 Snapshot 序列化为格式化的 JSON 字符串
func (s *Snapshot) ToJSON() (string, error) {
	// 创建一个可读性更强的时间格式转换
	type jsonSnapshot struct {
		Timestamp string     `json:"timestamp"`
		Basic     BasicStats `json:"basic"`
		IP        IPStats    `json:"ip"`
		MAC       MACStats   `json:"mac"`
		Port      PortStats  `json:"port"`
		Protocol  struct {
			Protocols []ProtocolCount `json:"protocols"`
		} `json:"protocol"`
		Application struct {
			Apps []ApplicationCount `json:"apps"`
		} `json:"application"`
		TCPFlags struct {
			Flags []TCPFlagCount `json:"flags"`
		} `json:"tcp_flags"`
	}

	// 创建JSON结构
	jsonData := jsonSnapshot{
		Timestamp: time.Now().UTC().Format("2006-01-02 15:04:05"),
		Basic:     s.Basic,
		IP:        s.IP,
		MAC:       s.MAC,
		Port:      s.Port,
	}

	// 复制Protocol数据
	jsonData.Protocol.Protocols = s.Protocol.Protocols

	// 复制Application数据
	jsonData.Application.Apps = s.Application.Apps

	// 复制TCPFlags数据
	jsonData.TCPFlags.Flags = s.TCPFlags.Flags

	// 序列化为带缩进的JSON
	jsonBytes, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return "", fmt.Errorf("序列化Snapshot失败: %w", err)
	}

	return string(jsonBytes), nil
}

// ToCompactJSON 将 Snapshot 序列化为紧凑的 JSON 字符串
func (s *Snapshot) ToCompactJSON() (string, error) {
	jsonBytes, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("序列化Snapshot失败: %w", err)
	}

	return string(jsonBytes), nil
}
