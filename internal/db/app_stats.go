package db

import (
	"context"
	"database/sql"
	"fmt"

	"SnapFlow/internal/models"
)

// FillApplicationStats 填充应用层协议统计到snapshot中
func FillApplicationStats(ctx context.Context, db *sql.DB, tableName string, snapshot *models.Snapshot) error {
	// 使用WITH语句计算应用层协议分布和百分比
	query := fmt.Sprintf(`
		WITH total_packets AS (
			-- 计算总的数据包数量
			SELECT COUNT(*) AS total_count
			FROM %s
			WHERE ts >= NOW() - INTERVAL 1 MINUTE
		)
		SELECT 
			application AS name, 
			COUNT(*) AS count, 
			(COUNT(*) * 100.0 / (SELECT total_count FROM total_packets)) AS percentage
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY application
		ORDER BY count DESC;
	`, tableName, tableName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("获取应用层协议统计失败: %w", err)
	}
	defer rows.Close()

	// 存储应用层协议统计
	var applications []models.ApplicationCount

	// 扫描应用层协议数据
	for rows.Next() {
		var app models.ApplicationCount
		var name sql.NullString // 使用NullString处理可能为NULL的应用名称

		if err := rows.Scan(&name, &app.Count, &app.Percentage); err != nil {
			return fmt.Errorf("扫描应用层协议数据失败: %w", err)
		}

		// 处理NULL应用名称
		if name.Valid {
			app.Name = name.String
		} else {
			app.Name = "unknown" // 将NULL值替换为"unknown"
		}

		applications = append(applications, app)
	}

	// 检查扫描错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("扫描应用层协议数据时发生错误: %w", err)
	}

	// 设置应用层协议统计
	snapshot.Application.Apps = applications

	return nil
}

// GetTopApplications 获取前N个最常用的应用层协议
func GetTopApplications(ctx context.Context, db *sql.DB, tableName string, limit int) ([]models.ApplicationCount, error) {
	query := fmt.Sprintf(`
		WITH total_packets AS (
			SELECT COUNT(*) AS total_count
			FROM %s
			WHERE ts >= NOW() - INTERVAL 1 MINUTE
		)
		SELECT 
			application AS name, 
			COUNT(*) AS count, 
			(COUNT(*) * 100.0 / (SELECT total_count FROM total_packets)) AS percentage
		FROM %s
		WHERE ts >= NOW() - INTERVAL 1 MINUTE
		GROUP BY application
		ORDER BY count DESC
		LIMIT ?
	`, tableName, tableName)

	rows, err := db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("获取前%d个应用层协议失败: %w", limit, err)
	}
	defer rows.Close()

	var applications []models.ApplicationCount

	for rows.Next() {
		var app models.ApplicationCount
		var name sql.NullString

		if err := rows.Scan(&name, &app.Count, &app.Percentage); err != nil {
			return nil, fmt.Errorf("扫描应用层协议数据失败: %w", err)
		}

		if name.Valid {
			app.Name = name.String
		} else {
			app.Name = "unknown"
		}

		applications = append(applications, app)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return applications, nil
}
