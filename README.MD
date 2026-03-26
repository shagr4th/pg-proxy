`pg-proxy` is a postgres protocol Proxy server, able to translate on the fly, some incompatible queries coming from a legacy application used with another database engine

## Usage

```
Usage of ./pg-proxy:
  -certificate string
        SSL certificate file *.crt (default none)
  -host string
        Listener host (default all local interfaces)
  -keeporiginal
        Keep the original query at the end in a multiline SQL comment (default true)
  -key string
        SSL key file *.key (default none)
  -parameters string
        Default client parameters (without override of the client's ones), in JSON format (default "{}")
  -polyfill
        Polyfills already applied by a system account (default true)
  -port int
        Listener port (default 5432)
  -remote string
        Proxy remote address (default none)
  -translator string
        Query translator ('ingres' or 'iso') (default "iso")
  -verbose int
        Verbosity: 0 = none, 1 = connections, 2 = translated queries, 4 = all queries (default 0)
  -web-port int
        Web UI port for query monitoring (0 = disabled)
  -web-secret string
        Web UI secret (default none)
```

## PGBROKER

the pgbroker folder is a copy of the [original project](https://github.com/rueian/pgbroker), with two small changes:

* Messages of the types `copyin`, `copyout`, `copydata`, `copydone`, coming from the server, can be skipped, so ce wan implement a different copy protocol management (so we can make the copy files local to the application server, and not to the postgres server)
* a new variable `QueryContext` has been added to the original connection context