package db

// IP地址统计查询
const (
	// 获取指定时间范围内请求次数最多的源IP
	QueryTopSourceIPs = `
		SELECT src_ip, COUNT(*) AS request_count
		FROM %s
		WHERE ts >= ? AND ts <= ?
		GROUP BY src_ip
		ORDER BY request_count DESC
		LIMIT ?
	`

	// 获取指定时间间隔内请求次数最多的源IP
	QueryTopSourceIPsWithInterval = `
		SELECT src_ip, COUNT(*) AS request_count
		FROM %s
		WHERE ts >= NOW() - INTERVAL ? SECOND
		GROUP BY src_ip
		ORDER BY request_count DESC
		LIMIT ?
	`
)
