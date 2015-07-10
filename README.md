# go-redis-migrator
A cluster aware Redis key migrator.  Moves keys from one cluster or host to another cluster or host.

## Details
This program connects to a source host or cluster and migrates keys to a
destination host or cluster.  It is possible to use the -keyFilter flag to match
a subset of keys you wish to migrate. It is also possible to fetch a list of all
keys with this program, create a file with a subset of keys to migrate, and feed
that list back into this program for selective migration.  This is useful if you
have some keys hanging around that you do not want to migrate and want to prune
during the migration.

## Setup
First, get the dependencies: `go get -v`

Then, make a build: `go build`

Finally, run the binary to see the help: `./go-redis-migrator`

## A quick example (pun intended)

```
time ./go-redis-migrator -copyData -keyfilter=sessions:* -sourceHosts=172.31.37.164:6379,172.31.37.162:6379,172.31.37.168:6379,172.31.37.170:6379,172.31.37.169:6379 -destinationHosts=172.31.34.231:6379,172.31.34.228:6379,172.31.34.227:6379,172.31.34.230:6379,172.31.34.229:6379,172.31.34.226:6379
Migrated 57 keys.

real	0m0.135s
user	0m0.011s
sys		0m0.031s
```

## A speed test

```
time ./go-redis-migrator -sourceHosts=127.0.0.1:6379 -destinationHosts=127.0.0.1:6380 -copyData
Migrated 22000 keys.

real	0m3.455s
user	0m0.812s
sys		0m1.475s

*6,367 keys/sec*
```

## Examples

#### Getting a key list from the source
`./go-redis-migrator -getKeys -sourceHosts=127.0.0.1:6379`

#### With a key filter on it:

`./go-redis-migrator -getKeys -keyFilter=sessions:\* -sourceHosts=127.0.0.1:6379`

#### With a cluster:

`./go-redis-migrator -getKeys -keyFilter=sessions:\* -sourceHosts=127.0.0.1:6379,127.0.0.1:6380`

#### Migrating keys 
`./go-redis-migrator -copyData -sourceHosts=127.0.0.1:6379 -destinationHosts=192.168.0.1:6379`

#### With clusters:

`./go-redis-migrator -copyData -sourceHosts=127.0.0.1:6379,127.0.0.1:6380 -destinationHosts=192.168.0.1:6379,192.168.0.1:6380`

or from a cluster and to a single host:

`./go-redis-migrator -copyData -sourceHosts=127.0.0.1:6379,127.0.0.1:6380 -destinationHosts=192.168.0.1:6379`

#### From a single host to a cluster:

`./go-redis-migrator -copyData -sourceHosts=127.0.0.1:6379 -destinationHosts=192.168.0.1:6379,192.168.0.1:6380`

#### Migrating only keys from a list
`./go-redis-migrator -copyData -sourceHosts=127.0.0.1:6379 -destinationHosts=192.168.0.1:6379 -keyFile=./onlyMoveTheseKeys.txt`


## CLI help
Simply run the binary to get the following help:
```
- Redis Key Migrator - 
https://github.com/integrii/go-redis-migrator

Migrates all or some of the keys from a Redis host or cluster to a specified host or cluster.  Supports migrating TTLs.
go-redis-migrator can also be used to list the keys in a cluster.  Run with the -getKey and -sourceHosts= flags to do so.

You must run this program with an operation before it will do anything.

Flags:
  -getKeys=false: Fetches and prints keys from the source host.
  -copyData=false: Copies all keys in a list specified by -keyFile= to the destination cluster from the source cluster.
  -keyFile="": The file path which contains the list of keys to migrate.  One per line.
  -keyFilter="*": The filter for which keys to migrate.  Does not work when -keyFile is also specified.
  -destinationHosts="": A list of source cluster host:port servers seperated by spaces. Use a single host without a ',' if there is no cluster. EX) 127.0.0.1:6379,127.0.0.1:6380
  -sourceHosts="": A list of source cluster host:port servers seperated by commas. Use a single host without a ',' if there is no cluster. EX) 127.0.0.1:6379,127.0.0.1:6380
```
