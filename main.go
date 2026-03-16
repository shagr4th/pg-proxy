package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"schenker/pg-proxy/ingres"
	"schenker/pg-proxy/proxy"
	"syscall"
	"time"
)

var commit string

func main() {

	fmt.Println(`###################################
#    PostgreSQL Remote Proxy      #
###################################

Commit: ` + commit)

	instance := proxy.ProxyInstance{
		Host:            "",
		Port:            5432,
		Remote:          "",
		Verbose:         0,
		CertificateFile: "",
		KeyFile:         "",
	}

	var translator string
	var startupOverride string
	var webPort int
	var webSecret string
	flag.StringVar(&instance.Host, "host", "", "Listener host (default all local interfaces)")
	flag.IntVar(&instance.Port, "port", 5432, "Listener port")
	flag.IntVar(&instance.Verbose, "verbose", 0, "Verbosity: 0 = none, 1 = connections, 2 = translated queries, 4 = all queries, 8 = protocol debug (default 0)")
	flag.BoolVar(&instance.KeepOriginal, "keeporiginal", true, "Keep the original query at the end in a multiline SQL comment")
	flag.StringVar(&instance.Remote, "remote", "", "Proxy remote address (default none)")
	flag.StringVar(&instance.CertificateFile, "certificate", "", "SSL certificate file *.crt (default none)")
	flag.StringVar(&instance.KeyFile, "key", "", "SSL key file *.key (default none)")
	flag.StringVar(&translator, "translator", "iso", "Query translator ('ingres' or 'iso')")
	flag.StringVar(&startupOverride, "override", "{}", "Startup parameters override, in JSON format")
	flag.IntVar(&webPort, "web-port", 0, "Web UI port for query monitoring (0 = disabled)")
	flag.StringVar(&webSecret, "web-secret", "", "Web UI secret (default none)")
	flag.Parse()

	if translator == "ingres" {
		instance.Translator = ingres.IngresTranslator(false)
	}
	if startupOverride != "" {
		err := json.Unmarshal([]byte(startupOverride), &instance.StartupParametersOverride)
		if err != nil {
			log.Panic(err)
		}
	}
	if webPort > 0 {
		instance.StartWebServer(webPort, webSecret)
		log.Printf("[Web UI listening on :%d]", webPort)
	}

	server, err := instance.NewServer()
	if err != nil {
		log.Panic(err)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", instance.Host, instance.Port))
	if err != nil {
		log.Panic(err)
	}
	banner := "No remote host defined. Use 'database@remotehost:remoteport' as the database name !"
	if len(instance.Remote) > 0 {
		banner = fmt.Sprintf("proxying to %s", instance.Remote)
	}
	b, _ := json.Marshal(instance.StartupParametersOverride)
	log.Printf("[Listening on %s:%d with %s translator and %s startup parameters] "+banner, instance.Host, instance.Port, translator, string(b))
	go server.Serve(ln)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Println("Shutting down in 3 seconds...")
	go server.Shutdown()
	<-time.After(3 * time.Second)
}
