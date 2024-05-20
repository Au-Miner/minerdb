module minerdb

go 1.19

require ( // need modified
	github.com/bwmarrin/snowflake v0.3.0
	github.com/google/btree v1.1.2
	github.com/hashicorp/golang-lru/v2 v2.0.4
	github.com/valyala/bytebufferpool v1.0.0
)

require ( // MinerDB
	github.com/gofrs/flock v0.8.1
	github.com/robfig/cron/v3 v3.0.0
	golang.org/x/sys v0.11.0 // indirect
)

require ( // raft
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/hashicorp/go-hclog v1.4.0
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/hashicorp/raft v1.3.11
	github.com/hashicorp/raft-boltdb/v2 v2.2.2
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/narvikd/errorskit v1.0.0
	github.com/narvikd/filekit v1.0.1
	go.etcd.io/bbolt v1.3.5 // indirect
)

require ( // jin
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	github.com/stretchr/testify v1.9.0
)

require ( // jrpc
	github.com/samuel/go-zookeeper v0.0.0-20201211165307-7117e9ea2414
)