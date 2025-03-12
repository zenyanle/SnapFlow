package models

import (
	"time"
)

// PacketData 表示网络数据包记录
type PacketData struct {
	Timestamp   time.Time
	PacketSize  uint16
	EtherType   uint16
	SrcMAC      string
	DstMAC      string
	Protocol    uint8
	SrcIP       string
	DstIP       string
	SrcPort     uint16
	DstPort     uint16
	TCPFlags    uint8
	PacketType  string
	Application string
}
