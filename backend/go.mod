module github.com/ductor/ecommerce_app/backend

go 1.18

require (
	github.com/cloudwego/hertz v0.10.2
	github.com/jackc/pgx/v4 v4.18.3
	github.com/nats-io/nats.go v1.48.0
	github.com/prometheus/client_golang v1.17.0
	github.com/prometheus/common v0.44.0
	github.com/redis/go-redis/v9 v9.7.0
	golang.org/x/crypto v0.47.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bytedance/gopkg v0.1.1 // indirect
	github.com/bytedance/sonic v1.14.0 // indirect
	github.com/bytedance/sonic/loader v0.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.5 // indirect
	github.com/cloudwego/gopkg v0.1.4 // indirect
	github.com/cloudwego/netpoll v0.7.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/nats-io/nats-server/v2 v2.12.4 // indirect
	github.com/nats-io/nkeys v0.4.12 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/nyaruka/phonenumbers v1.0.55 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace golang.org/x/crypto => golang.org/x/crypto v0.17.0

replace github.com/nats-io/nats.go => github.com/nats-io/nats.go v1.22.1

replace golang.org/x/sys => golang.org/x/sys v0.24.0

replace golang.org/x/text => golang.org/x/text v0.14.0

replace google.golang.org/protobuf => google.golang.org/protobuf v1.34.1

replace github.com/nats-io/nkeys => github.com/nats-io/nkeys v0.3.0

replace github.com/nats-io/nats-server/v2 => github.com/nats-io/nats-server/v2 v2.10.18
