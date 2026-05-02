package main

import (
	"flag"
	"fmt"
	"log"
	"mvzserver/app"
	"mvzserver/constants"
	"mvzserver/utils"
	"os"
	"path/filepath"
)

func main() {
	// 定义命令行参数
	usePort := flag.String("p", "28080", "指定监听端口")
	useHost := flag.String("h", "", "指定监听地址")
	showVersion := flag.Bool("v", false, "显示版本信息")
	newLocalCerts := flag.String("new-local-certs", "", "在指定目录生成新的 ssl.cert 和 ssl.key")
	pemCertsDir := flag.String("pem-certs", "", "使用包含 cert.pem 和 privkey.pem 的证书目录")
	pemCertsDirShort := flag.String("c", "", "使用包含 cert.pem 和 privkey.pem 的证书目录")
	localCertsDir := flag.String("local-certs", "", "使用包含 ssl.cert 和 ssl.key 的证书目录，缺失时自动生成")
	localCertsDirShort := flag.String("l", "", "使用包含 ssl.cert 和 ssl.key 的证书目录，缺失时自动生成")
	flag.Parse()

	// 如果请求生成新的本地证书
	if *newLocalCerts != "" {
		fmt.Printf("正在生成新的本地证书到: %s\n", *newLocalCerts)
		if err := utils.GenerateNewCerts(*newLocalCerts); err != nil {
			log.Fatalf("Failed to generate new certificates: %v", err)
		}

		fmt.Println("新证书生成完成！")
		os.Exit(0)
	}

	// 如果请求显示版本信息
	if *showVersion {
		fmt.Printf("%s v%s\n", constants.AppName, constants.Version)
		os.Exit(0)
	}

	finalPemCertsDir := firstNonEmpty(*pemCertsDir, *pemCertsDirShort)
	finalLocalCertsDir := firstNonEmpty(*localCertsDir, *localCertsDirShort)
	if finalPemCertsDir != "" && finalLocalCertsDir != "" {
		log.Fatal("只能同时指定一种证书目录：--pem-certs/-c 或 --local-certs/-l")
	}

	if finalPemCertsDir == "" && finalLocalCertsDir == "" {
		defaultCertsDir, err := executableCertsDir()
		if err != nil {
			log.Fatalf("Failed to get executable certs directory: %v", err)
		}
		finalLocalCertsDir = defaultCertsDir
	}

	appConfig := app.AppConfig{
		Host:          *useHost,
		Port:          *usePort,
		PemCertsDir:   finalPemCertsDir,
		LocalCertsDir: finalLocalCertsDir,
	}

	app := app.NewApp(appConfig)
	if app == nil {
		log.Fatal("Failed to create app")
	}
	app.Run()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func executableCertsDir() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(executable), "certs"), nil
}
