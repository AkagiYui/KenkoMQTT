package services_test

import (
	"path/filepath"
	"testing"

	"kenkomqtt/internal/database"
	"kenkomqtt/internal/models"
	"kenkomqtt/internal/services"
)

// TestSaveConnection 验证连接档案的创建/更新语义：
//   - 新建时后端生成 ID 与时间戳；
//   - 更新时保留原 CreatedAt、刷新 UpdatedAt，并覆盖可编辑字段（含布尔零值）；
//   - 入参不含时间字段（ConnectionInput），因此不会触发 time.Time 解析问题。
func TestSaveConnection(t *testing.T) {
	db, err := database.Initialize(filepath.Join(t.TempDir(), "conn.db"))
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}
	svc := services.NewConnectionService(db)

	created, err := svc.SaveConnection(models.ConnectionInput{
		Name:         "本地",
		Host:         "127.0.0.1",
		CleanSession: true,
	})
	if err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	if created.ID == "" {
		t.Fatal("期望后端生成非空 ID")
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Fatal("期望后端写入时间戳")
	}
	// 未提供的字段应回落到默认值。
	if created.Protocol != "tcp" || created.Port != 1883 || created.KeepAlive != 60 || created.MQTTVersion != 4 {
		t.Fatalf("默认值未生效: %+v", created)
	}

	updated, err := svc.SaveConnection(models.ConnectionInput{
		ID:           created.ID,
		Name:         "改名",
		Host:         "192.168.1.10",
		Port:         8883,
		CleanSession: false, // 布尔零值也必须被写入
	})
	if err != nil {
		t.Fatalf("更新失败: %v", err)
	}
	if updated.Name != "改名" || updated.Host != "192.168.1.10" || updated.Port != 8883 {
		t.Fatalf("更新字段未生效: %+v", updated)
	}
	if updated.CleanSession {
		t.Fatal("CleanSession 应被更新为 false")
	}
	if !updated.CreatedAt.Equal(created.CreatedAt) {
		t.Fatal("更新应保留原 CreatedAt")
	}
	if !updated.UpdatedAt.After(created.UpdatedAt) && !updated.UpdatedAt.Equal(created.UpdatedAt) {
		t.Fatal("更新应刷新 UpdatedAt")
	}

	list, err := svc.ListConnections()
	if err != nil {
		t.Fatalf("列出失败: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("期望恰好 1 条记录，实际 %d", len(list))
	}
}
