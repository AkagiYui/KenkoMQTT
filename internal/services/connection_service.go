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
// 时间戳完全由后端维护：新建时写入 CreatedAt/UpdatedAt，更新时保留原 CreatedAt、刷新 UpdatedAt。
func (s *ConnectionService) SaveConnection(input models.ConnectionInput) (*models.Connection, error) {
	if input.Protocol == "" {
		input.Protocol = "tcp"
	}
	if input.Port == 0 {
		input.Port = 1883
	}
	if input.KeepAlive == 0 {
		input.KeepAlive = 60
	}
	if input.MQTTVersion == 0 {
		input.MQTTVersion = 4
	}

	now := time.Now()

	if input.ID == "" {
		conn := models.Connection{
			ID:            uuid.NewString(),
			Name:          input.Name,
			Protocol:      input.Protocol,
			Host:          input.Host,
			Port:          input.Port,
			Path:          input.Path,
			ClientID:      input.ClientID,
			Username:      input.Username,
			Password:      input.Password,
			KeepAlive:     input.KeepAlive,
			CleanSession:  input.CleanSession,
			MQTTVersion:   input.MQTTVersion,
			TLSSkipVerify: input.TLSSkipVerify,
			SortOrder:     input.SortOrder,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := s.db.Create(&conn).Error; err != nil {
			return nil, fmt.Errorf("创建连接档案失败: %w", err)
		}
		return &conn, nil
	}

	var conn models.Connection
	if err := s.db.Where("id = ?", input.ID).First(&conn).Error; err != nil {
		return nil, fmt.Errorf("待更新的连接档案不存在: %w", err)
	}
	conn.Name = input.Name
	conn.Protocol = input.Protocol
	conn.Host = input.Host
	conn.Port = input.Port
	conn.Path = input.Path
	conn.ClientID = input.ClientID
	conn.Username = input.Username
	conn.Password = input.Password
	conn.KeepAlive = input.KeepAlive
	conn.CleanSession = input.CleanSession
	conn.MQTTVersion = input.MQTTVersion
	conn.TLSSkipVerify = input.TLSSkipVerify
	conn.SortOrder = input.SortOrder
	conn.UpdatedAt = now
	if err := s.db.Save(&conn).Error; err != nil {
		return nil, fmt.Errorf("更新连接档案失败: %w", err)
	}
	return &conn, nil
}

// DeleteConnection 删除指定连接档案。
func (s *ConnectionService) DeleteConnection(id string) error {
	return s.db.Where("id = ?", id).Delete(&models.Connection{}).Error
}
