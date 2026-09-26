package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-orz/orz"
	"github.com/pika-monitor/pika/internal"
	"github.com/pika-monitor/pika/internal/config"
	"github.com/pika-monitor/pika/internal/database"
	v0_0_13 "github.com/pika-monitor/pika/internal/migrate/v0_0_13"
	"github.com/pika-monitor/pika/internal/vmclient"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gorm.io/gorm"

	_ "github.com/go-orz/orz/drivers/postgres"
	_ "github.com/go-orz/orz/drivers/sqlite"
)

var (
	configFile string
	rootCmd    = &cobra.Command{
		Use:   "pika-server",
		Short: "Pika 服务器监控系统",
		Long:  `Pika 是一个轻量级的服务器监控系统，支持多探针部署和实时监控。`,
		Run: func(cmd *cobra.Command, args []string) {
			internal.Run(configFile)
		},
	}

	serveCmd = &cobra.Command{
		Use:   "serve",
		Short: "启动 Pika 服务器",
		Long:  `启动 Pika HTTP 服务器，提供 Web 界面和 API 服务。`,
		Run: func(cmd *cobra.Command, args []string) {
			internal.Run(configFile)
		},
	}

	migrateCmd = &cobra.Command{
		Use:   "migrate_v0_0_13",
		Short: "执行数据库迁移",
		Long:  `将历史 PostgreSQL 指标数据迁移到 VictoriaMetrics（仅需执行一次）。`,
		Run: func(cmd *cobra.Command, args []string) {
			runMigration(configFile)
		},
	}
)

func init() {
	// 添加全局配置文件参数
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "./config.yaml", "配置文件路径")

	// 添加子命令
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(migrateCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}
}

// runMigration 执行数据迁移
func runMigration(configPath string) {
	fmt.Println("=== Pika 数据迁移工具 ===")
	fmt.Println("配置文件:", configPath)
	fmt.Println()

	// 初始化 orz 以获取数据库连接和配置
	var db *gorm.DB
	var vmClient *vmclient.VMClient
	var logger *zap.Logger

	err := orz.Quick(configPath, func(app *orz.App) error {
		logger = app.Logger()
		db = app.GetDatabase()
		if err := database.ConfigureConnectionPool(db, logger); err != nil {
			return err
		}

		// 读取 VictoriaMetrics 配置
		var appConfig config.AppConfig
		_config := app.GetConfig()
		if _config != nil {
			if err := _config.App.Unmarshal(&appConfig); err != nil {
				return fmt.Errorf("读取配置失败: %w", err)
			}
		}

		// 初始化 VictoriaMetrics 客户端
		vmClient = provideVMClient(&appConfig, logger)

		return nil
	})

	if err != nil {
		log.Fatalf("初始化失败: %v", err)
	}

	// 执行迁移
	fmt.Println("开始迁移历史指标数据到 VictoriaMetrics...")
	fmt.Println("说明: 数据将从 PostgreSQL 迁移到 VictoriaMetrics。")
	fmt.Println("警告: 建议在迁移前备份 PostgreSQL 数据！")
	fmt.Println()
	fmt.Print("是否在迁移完成后自动删除旧的数据表？(yes/no): ")

	var deleteOldTables bool
	var answer string
	fmt.Scanln(&answer)
	if answer == "yes" || answer == "y" || answer == "Y" {
		deleteOldTables = true
		fmt.Println("✓ 将在迁移完成后自动删除旧表")
	} else {
		deleteOldTables = false
		fmt.Println("✓ 将保留旧表，您可以稍后手动删除")
	}

	fmt.Println()
	startTime := time.Now()

	// 执行数据迁移
	if err := v0_0_13.Migrate(db, vmClient); err != nil {
		logger.Error("迁移失败", zap.Error(err))
		log.Fatalf("迁移失败: %v", err)
	}

	elapsed := time.Since(startTime)
	fmt.Println()
	fmt.Printf("✓ 数据迁移成功完成！耗时: %s\n", elapsed)

	// 根据用户选择决定是否删除旧表
	if deleteOldTables {
		fmt.Println()
		fmt.Println("正在删除旧的指标数据表...")
		if err := v0_0_13.DropOldTables(db); err != nil {
			logger.Error("删除旧表失败", zap.Error(err))
			log.Printf("警告: 删除旧表失败: %v\n", err)
			fmt.Println("注意: 旧表删除失败，但数据已成功迁移，您可以手动删除这些表。")
		} else {
			fmt.Println("✓ 旧表删除完成")
			fmt.Println()
			fmt.Println("✓ 迁移任务全部完成！")
		}
	} else {
		fmt.Println()
		fmt.Println("提示: 旧数据表已保留，您可以验证数据后手动删除。")
		fmt.Println("提示: 如需删除旧表，可以重新运行此命令并选择删除。")
	}

	fmt.Println()
	fmt.Println("提示: 现在可以启动服务器: ./pika-server serve")
}

// provideVMClient 提供 VictoriaMetrics 客户端
func provideVMClient(cfg *config.AppConfig, logger *zap.Logger) *vmclient.VMClient {
	// 检查配置
	if cfg.VictoriaMetrics == nil || !cfg.VictoriaMetrics.Enabled {
		logger.Info("VictoriaMetrics is not enabled, using default configuration")
		// 返回一个默认配置的客户端（用于本地开发）
		return vmclient.NewVMClient("http://localhost:8428", 30*time.Second, 60*time.Second)
	}

	// 使用配置创建客户端
	writeTimeout := time.Duration(cfg.VictoriaMetrics.WriteTimeout) * time.Second
	if writeTimeout == 0 {
		writeTimeout = 30 * time.Second
	}

	queryTimeout := time.Duration(cfg.VictoriaMetrics.QueryTimeout) * time.Second
	if queryTimeout == 0 {
		queryTimeout = 60 * time.Second
	}

	logger.Info("VictoriaMetrics client initialized",
		zap.String("url", cfg.VictoriaMetrics.URL),
		zap.Duration("writeTimeout", writeTimeout),
		zap.Duration("queryTimeout", queryTimeout))

	return vmclient.NewVMClient(cfg.VictoriaMetrics.URL, writeTimeout, queryTimeout)
}
