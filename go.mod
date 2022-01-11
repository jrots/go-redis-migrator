module github.com/integrii/go-redis-migrator

go 1.16

require (
	github.com/go-redis/redis/v8 v8.0.0-00010101000000-000000000000
	github.com/tidwall/rhh v1.3.0 // indirect
)

replace github.com/go-redis/redis/v8 => ../redis-1
