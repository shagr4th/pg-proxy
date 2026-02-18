package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var commit string

func main() {

	fmt.Println(`###################################
#    PostgreSQL Remote Proxy      #
###################################

Commit: ` + commit)

	proxyConfig := ProxyConfig{
		Host:            "",
		Port:            5432,
		Remote:          "",
		Verbose:         0,
		Polyfilled:      false,
		CertificateFile: "",
		KeyFile:         "",
	}

	var translator string
	var parameters string
	var webPort int
	var webUser, webPass string
	flag.StringVar(&proxyConfig.Host, "host", "", "Listener host (default all local interfaces)")
	flag.IntVar(&proxyConfig.Port, "port", 5432, "Listener port")
	flag.IntVar(&proxyConfig.Verbose, "verbose", 0, "Verbosity: 0 = none, 1 = connections, 2 = translated queries, 4 = all queries (default 0)")
	flag.BoolVar(&proxyConfig.Polyfilled, "polyfill", true, "Polyfills already applied by a system account")
	flag.StringVar(&proxyConfig.Remote, "remote", "", "Proxy remote address (default none)")
	flag.StringVar(&proxyConfig.CertificateFile, "certificate", "", "SSL certificate file *.crt (default none)")
	flag.StringVar(&proxyConfig.KeyFile, "key", "", "SSL key file *.key (default none)")
	flag.StringVar(&translator, "translator", "ingres", "Query translator")
	flag.StringVar(&parameters, "parameters", "{\"datestyle\":\"iso,us\"}", "Startup parameters") // on force datestyle pour simuler le date_format=US par défaut dans la base Ingres
	flag.IntVar(&webPort, "web-port", 0, "Web UI port for query monitoring (0 = disabled)")
	flag.StringVar(&webUser, "web-user", "", "Web UI basic auth username (default none)")
	flag.StringVar(&webPass, "web-pass", "", "Web UI basic auth password (default none)")
	flag.Parse()

	if translator == "ingres" {
		proxyConfig.SqlTranslator = IngresTranslator()
	}
	if parameters != "" {
		err := json.Unmarshal([]byte(parameters), &proxyConfig.StartupParameters)
		if err != nil {
			log.Panic(err)
		}
	}
	if webPort > 0 {
		store := NewQueryStore()
		proxyConfig.QueryStore = store
		StartWebServer(webPort, store, webUser, webPass)
		log.Printf("[Web UI listening on :%d]", webPort)
	}

	server, err := proxyConfig.NewServer()
	if err != nil {
		log.Panic(err)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", proxyConfig.Host, proxyConfig.Port))
	if err != nil {
		log.Panic(err)
	}
	banner := "No remote host defined. Use 'database@remotehost:remoteport' as the database name !"
	if len(proxyConfig.Remote) > 0 {
		banner = fmt.Sprintf("proxying to %s", proxyConfig.Remote)
	}
	log.Printf("[Listening on %s:%d with %s translator] "+banner, proxyConfig.Host, proxyConfig.Port, translator)
	go server.Serve(ln)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Println("Shutting down in 3 seconds...")
	go server.Shutdown()
	<-time.After(3 * time.Second)
}
