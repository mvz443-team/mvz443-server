package app

import (
	"crypto/tls"
	"fmt"
	"log"
	"mvzserver/handlers"
	roommanager "mvzserver/room-manager"
	"mvzserver/utils"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/websocket/v2"
)

type App struct {
	fiberApp    *fiber.App   // Fiber 应用实例
	NetListener net.Listener // 网络监听器

	RoomManager *roommanager.RoomManager // 房间管理器
}

type AppConfig struct {
	Host          string // 指定监听地址
	Port          string // 指定监听端口
	PemCertsDir   string // 包含 cert.pem 和 privkey.pem 的证书目录
	LocalCertsDir string // 包含 ssl.cert 和 ssl.key 的证书目录
}

func NewApp(cfg AppConfig) *App {
	// 新建room manager
	var roomManager = roommanager.NewRoomManager()

	// 新建网络服务
	// 网络服务在内部再设置监听
	var serverHandler = handlers.NewServerHandler(roomManager)
	fiberApp, ln := CreateFiberApp(cfg, serverHandler)

	return &App{
		fiberApp:    fiberApp,
		NetListener: ln,
		RoomManager: roomManager,
	}
}

// 创建 Fiber 应用
func CreateFiberApp(cfg AppConfig, serverHandler *handlers.ServerHandler) (fapp *fiber.App, ln net.Listener) {
	var err error
	fapp = fiber.New()
	// 使用 CORS 中间件配置
	fapp.Use(cors.New(cors.Config{
		AllowOrigins:     "*", // 如需限制域名，请修改此处
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin, Content-Type, Accept",
		AllowCredentials: false,
		MaxAge:           3600,
	}))

	// 注册路由（包括 websocket 和其它接口）
	// ALLOWED_ORIGINS 未设置时保持原有宽松行为；设置后按逗号分隔白名单校验 Origin。
	wsConfig := websocket.Config{}
	if origins := strings.TrimSpace(os.Getenv("ALLOWED_ORIGINS")); origins != "" {
		wsConfig.Origins = strings.Split(origins, ",")
		for i := range wsConfig.Origins {
			wsConfig.Origins[i] = strings.TrimSpace(wsConfig.Origins[i])
		}
	}
	fapp.Get("/ws", websocket.New(serverHandler.HandleWS, wsConfig))
	fapp.Get("/list", serverHandler.HandleListRoom)
	// 客户端联机设置里的连通性与延时探测
	fapp.Get("/ping", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"pong": time.Now().UnixMilli()})
	})

	listenAddr := net.JoinHostPort(cfg.Host, cfg.Port)
	certPath, keyPath := resolveCertPair(cfg)

	log.Printf("启用 HTTPS 服务，监听 %s，使用证书: %s", listenAddr, certPath)

	cer, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		log.Fatal("加载证书失败:", err)
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cer}}
	ln, err = tls.Listen("tcp", listenAddr, tlsConfig)
	if err != nil {
		log.Fatal("监听端口失败:", err)
	}

	return fapp, ln
}

func (app *App) Run() {
	log.Fatal(app.fiberApp.Listener(app.NetListener))
}

func resolveCertPair(cfg AppConfig) (string, string) {
	if cfg.PemCertsDir != "" {
		return filepath.Join(cfg.PemCertsDir, "cert.pem"), filepath.Join(cfg.PemCertsDir, "privkey.pem")
	}

	certPath := filepath.Join(cfg.LocalCertsDir, "ssl.cert")
	keyPath := filepath.Join(cfg.LocalCertsDir, "ssl.key")
	if _, err := os.Stat(certPath); err == nil {
		if _, err := os.Stat(keyPath); err == nil {
			return certPath, keyPath
		}
	}

	fmt.Printf("本地证书不存在，正在生成到: %s\n", cfg.LocalCertsDir)
	if err := utils.GenerateNewCerts(cfg.LocalCertsDir); err != nil {
		log.Fatalf("Failed to generate local certificates: %v", err)
	}
	return certPath, keyPath
}
