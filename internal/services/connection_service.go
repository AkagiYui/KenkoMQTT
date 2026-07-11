package services

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"kenkomqtt/internal/models"
)

// ConnectionService 管理已保存的客户端连接档案（持久化到数据库）。
type ConnectionService struct {
	db *gorm.DB
}

// NewConnectionService 创建连接档案服务实例。
func NewConnectionService(db *gorm.DB) *ConnectionService {
	return &ConnectionService{db: db}
}

// ListConnections 返回全部连接档案，按排序字段与创建时间升序。
func (s *ConnectionService) ListConnections() ([]models.Connection, error) {
	var conns []models.Connection
	if err := s.db.Order("sort_order asc, created_at asc").Find(&conns).Error; err != nil {
		return nil, err
	}
	return conns, nil
}

// GetConnection 按 ID 获取单个连接档案。
func (s *ConnectionService) GetConnection(id string) (*models.Connection, error) {
	var conn models.Connection
	if err := s.db.Where("id = ?", id).First(&conn).Error; err != nil {
		return nil, err
	}
	return &conn, nil
}

// SaveConnection 创建或更新连接档案。ID 为空时创建新记录并返回。
func (s *ConnectionService) SaveConnection(conn models.Connection) (*models.Connection, error) {
	if conn.Protocol == "" {
		conn.Protocol = "tcp"
	}
	if conn.Port == 0 {
		conn.Port = 1883
	}
	if conn.KeepAlive == 0 {
		conn.KeepAlive = 60
	}
	if conn.MQTTVersion == 0 {
		conn.MQTTVersion = 4
	}

	if conn.ID == "" {
		conn.ID = uuid.NewString()
		conn.CreatedAt = time.Now()
		conn.UpdatedAt = conn.CreatedAt
		if err := s.db.Create(&conn).Error; err != nil {
			return nil, fmt.Errorf("创建连接档案失败: %w", err)
		}
		return &conn, nil
	}

	conn.UpdatedAt = time.Now()
	if err := s.db.Model(&models.Connection{}).Where("id = ?", conn.ID).Save(&conn).Error; err != nil {
		return nil, fmt.Errorf("更新连接档案失败: %w", err)
	}
	return &conn, nil
}

// DeleteConnection 删除指定连接档案。
func (s *ConnectionService) DeleteConnection(id string) error {
	return s.db.Where("id = ?", id).Delete(&models.Connection{}).Error
}
