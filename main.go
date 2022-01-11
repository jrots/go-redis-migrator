package main

//
// Migrate data from one redis cluster to another.
//

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"crypto/sha1"

	"github.com/tidwall/rhh"
	"github.com/go-redis/redis/v8"

)

// redis server connections
var sourceCluster *redis.ClusterClient
var destinationCluster *redis.ClusterClient
var sourceHost *redis.Client
var destinationHost *redis.Client

// redis server arrays for connecting
var sourceHostsString string
var sourceHostsArray []string

var destinationHostsString string
var destinationHostsArray []string
var checkSumMap rhh.Map

var usePipeline = false;

// cluster indication flags
var sourceIsCluster = false
var destinationIsCluster = false

// connected states
var sourceClusterConnected = false
var destinationClusterConnected = false

// counter for number of keys migrated
var keysMigrated int64
var keysSkipped int64

var keyPipeline redis.Pipeliner = nil

var checkSumScanLua = redis.NewScript(`
local shas = {};
local t = {};
local done = false;
local cursor = ARGV[1];
local value = "";
local val = "";
local result = redis.call("SCAN", cursor, "count", ARGV[2])
cursor = result[1];
t = result[2];

for i=1,#t do
 local d = t[i];
 shas[#shas+1] = redis.sha1hex(redis.call('dump', d))
end;
 
return {cursor, t, shas}
`)

func main() {
	ctx := context.Background()

	//
	// Command line argument handling
	//

	// parse command line arguments
	var sourceHosts = flag.String("sourceHosts", "", "A list of source cluster host:port servers seperated by commas. EX) 127.0.0.1:6379,127.0.0.1:6380")
	var destinationHosts = flag.String("destinationHosts", "", "A list of source cluster host:port servers seperated by spaces. EX) 127.0.0.1:6379,127.0.0.1:6380")
	var getKeys = flag.Bool("getKeys", false, "Fetches and prints keys from the source host.")
	var copyData = flag.Bool("copyData", false, "Copies all keys in a specified list to the destination cluster from the source cluster.")
	usePipeline = *flag.Bool("usePipeline", true, "Speed up process using pipeline.")

	var keyFilePath = flag.String("keyFile", "", "The file path which contains the list of keys to migrate.")
	var keyFilter = flag.String("keyFilter", "*", "The pattern of keys to migrate if no key file path was specified.")

	// parse the args we are looking for
	flag.Parse()

	// Ensure a valid operation was passed
	if *getKeys == false && *copyData == false {
		showHelp()
	}

	//
	// Connect to redis servers or clusters
	//

	// connect to the host if servers found
	// break source hosts comma list into an array
	sourceHostsString = *sourceHosts
	if len(sourceHostsString) > 0 {
		//log.Println("Source hosts detected: " + sourceHostsString)
		// break source hosts string at commas into a slice
		sourceHostsArray = strings.Split(sourceHostsString, "&addr")
		connectSourceCluster(ctx)
	}

	// connect to the host if servers found
	// break destination hosts comma list into an array
	destinationHostsString = *destinationHosts
	if len(destinationHostsString) > 0 {
		//log.Println("Destination hosts detected: " + destinationHostsString)
		// break source hosts string at commas into a slice
		destinationHostsArray = strings.Split(destinationHostsString, "&addr")
		connectDestinationCluster(ctx)
	}

	loadDestinationChecksum(ctx);

	//
	// Do the right thing depending on the operations passed from cli
	//

	// Get and display a key list
	if *getKeys == true {

		// ensure we are connected
		if sourceClusterConnected != true {
			log.Fatalln("Please specify a source cluster using -sourceCluster=127.0.0.1:6379.")
		}

		//log.Println("Getting full key list...")
		// iterate through each host in the destination cluster, connect, and
		// run KEYS search
		var allKeys = getSourceKeys(ctx, *keyFilter)

		// see how many keys we fetched
		if len(allKeys) > 0 {
			// loop through all keys and print them plainly one per line
			for i := 0; i < len(allKeys); i++ {
				fmt.Println(allKeys[i])
			}
		} else {
			fmt.Println("No keys found in source cluster.")
		}
	}

	// Copy all or some keys to the new server/cluster
	if *copyData == true {

		// ensure we are connected
		if sourceClusterConnected != true {
			log.Fatalln("Please specify a source cluster using -sourceCluster=127.0.0.1:6379.")
			showHelp()
		}
		if destinationClusterConnected != true {
			log.Fatalln("Please specify a destination cluster using -destinationCluster=127.0.0.1:6379")
			showHelp()
		}

		// if the key file path was set, open the file
		if len(*keyFilePath) > 0 {

			// ensure that keyFile and keyFilter are not both specified
			if *keyFilter != "*" {
				log.Fatalln("Can not use -keyFilter= option with -keyFile= option.")
			}

			var keyFile, err = os.Open(*keyFilePath)
			if err != nil {
				log.Fatalln("Unable to open key file specified.")
			}
			// create a new scanner for parsing the io reader returned by the
			// os.Open call earlier
			var keyFileScanner = bufio.NewScanner(keyFile)

			// read the entire key file
			for keyFileScanner.Scan() {

				// fetch the text for this line
				var key = keyFileScanner.Text()

				// migrate the key from source to destination
				fmt.Println(checkSumMap.Get(shasum(key)))
				//migrateKey(ctx, key)

			}
		} else {
			if sourceIsCluster {
				sourceCluster.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
					var err error
					cursor := "0"
					for {
						keys := []string{}
						values := []interface{}{cursor, 10000}
						res, _ := checkSumScanLua.Run(ctx, rdb, keys, values...).Result()
						if x, ok := res.([]interface{}); ok {
							cursor = fmt.Sprintf("%v", x[0])
							if y, ok := x[1].([]interface{}); ok {
								if z, ok := x[2].([]interface{}); ok {
									for index, val := range y {
										foundKey := fmt.Sprintf("%v", val)
										checkSumVal, found := checkSumMap.Get(shasum(foundKey))
										if found == false || fmt.Sprintf("%v", checkSumVal) !=  fmt.Sprintf("%v",z[index]) {
											migrateKey(ctx, foundKey)
										} else {
											keysSkipped = keysSkipped + 1
										}
									}
								}
							}
						} else {
							break;
						}

						if cursor == "0" { // no more keys
							break
						}
					}
					return err
				})
				if usePipeline && keyPipeline != nil && keyPipeline.Len() > 0 {
					keyPipeline.Exec(ctx)
				}

			} else {
				panic("implement")
			}
		}

	}

	// Finish up with some stats
	log.Println("Migrated " + strconv.FormatInt(keysMigrated, 10) + " keys, Skipped "+ strconv.FormatInt(keysSkipped, 10))
}

// ping testing functions
func clusterPingTest(ctx context.Context, redisClient *redis.ClusterClient) {
	var pingTest = redisClient.Ping(ctx)
	var pingMessage, pingError = pingTest.Result()
	if pingError != nil {
		fmt.Println(pingError)
		log.Fatalln("Error when pinging a Redis connection:" + pingMessage)
	}
}
func hostPingTest(ctx context.Context, redisClient *redis.Client) {
	var pingTest = redisClient.Ping(ctx)
	var pingMessage, pingError = pingTest.Result()
	if pingError != nil {
		//fmt.Println(pingError)
		log.Fatalln("Error when pinging a Redis connection:" + pingMessage)
	}
}

// Connects to the source host/cluster
func connectSourceCluster(ctx context.Context) {

	// connect to source cluster and ping it
	if len(sourceHostsArray) == 1 {
		opts, err := redis.ParseURL(sourceHostsArray[0])
		if err != nil {
			panic("Error parsing redis url")
		}
		sourceHost = redis.NewClient(opts)
		sourceIsCluster = false
		//log.Println("Source is a single host.")
		hostPingTest(ctx, sourceHost)
	} else {
		opts, err := redis.ParseClusterURL(sourceHostsString)
		if err != nil {
			panic("Error parsing redis url")
		}
		sourceCluster = redis.NewClusterClient(opts)
		sourceIsCluster = true

		clusterPingTest(ctx, sourceCluster)
	}
	sourceClusterConnected = true
}

func shasum(fn string) string {
	h := sha1.New()
	h.Write([]byte(fn))
	bs := h.Sum(nil)
	return fmt.Sprintf("%x", bs)
}

// Connects to the destination host/cluster
func connectDestinationCluster(ctx context.Context) {

	// connect to destination cluster and ping it
	if len(destinationHostsArray) == 1 {
		opts, err := redis.ParseURL(destinationHostsArray[0])
		if err != nil {
			panic("Error parsing redis url")
		}
		destinationHost = redis.NewClient(opts)
		destinationIsCluster = false
		//log.Println("Destination is a single host.")
		hostPingTest(ctx, destinationHost)
	} else {

		opts, err := redis.ParseClusterURL(destinationHostsString)
		if err != nil {
			panic("Error parsing redis url")
		}
		destinationCluster = redis.NewClusterClient(opts)
		destinationIsCluster = true
		//log.Println("Destination is a cluster.")
		clusterPingTest(ctx, destinationCluster)
	}
	destinationClusterConnected = true
	//log.Println("Destination connected.")
}

func loadDestinationChecksum(ctx context.Context) {

	if destinationIsCluster {
		destinationCluster.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
			var err error
			cursor := "0"
			for {
				keys := []string{}
				values := []interface{}{cursor, 10000}
				res, _ := checkSumScanLua.Run(ctx, rdb, keys, values...).Result()
				if x, ok := res.([]interface{}); ok {
					cursor = fmt.Sprintf("%v", x[0])
					if y, ok := x[1].([]interface{}); ok {
						if z, ok := x[2].([]interface{}); ok {
							for index, val := range y {
								checkSumMap.Set(shasum(fmt.Sprintf("%v", val)), z[index])
							}
						}
					}
				} else {
					break
				}
				log.Println("Loaded: " + strconv.Itoa(checkSumMap.Len())  + " destination checksum keys cursor:" + cursor)

				if cursor == "0" { // no more keys
					break
				}
			}
			return err
		})
	} else {
		panic("implement")
	}
}

// Gets all the keys from the source server/cluster
func getSourceKeys(ctx context.Context, keyFilter string) []string {
	var allKeys *redis.StringSliceCmd
	if sourceIsCluster == true {
		allKeys = sourceCluster.Keys(ctx, keyFilter)
	} else {
		allKeys = sourceHost.Keys(ctx, keyFilter)
	}

	return allKeys.Val()
}

// Migrates a key from the source cluster to the deestination one
func migrateKey(ctx context.Context, key string) {

	keysMigrated = keysMigrated + 1

	// init a value to hold the key data
	var data string

	// init a value to hold the key ttl
	var ttl time.Duration

	// get the key from the source
	if sourceIsCluster == true {
		data = sourceCluster.Dump(ctx, key).Val()
		ttl = sourceCluster.PTTL(ctx, key).Val()

	} else {
		data = sourceHost.Dump(ctx, key).Val()
		ttl = sourceHost.PTTL(ctx, key).Val()
	}

	// set ttl to 0 due to restore requiring >= 0 for ttl
	// since ttl comes as -1000000 instead of -1 check everything less than 0
	if ttl < 0 {
		ttl = 0
	}

	// put the key in the destination cluster and set the ttl
	if destinationIsCluster == true {
		if usePipeline {
			if keyPipeline == nil {
				keyPipeline = destinationCluster.Pipeline()
			} else if keyPipeline.Len() > 1000  {
				_, err := keyPipeline.Exec(ctx)
				if err != nil {
					fmt.Println(err)
				}
				log.Println("Migrated " + strconv.FormatInt(keysMigrated, 10) + " keys, Skipped "+ strconv.FormatInt(keysSkipped, 10))
				keyPipeline = destinationCluster.Pipeline()
			}
			keyPipeline.RestoreReplace(ctx, key, ttl, data)
		} else {
			destinationCluster.RestoreReplace(ctx, key, ttl, data)
		}

	} else {
		destinationHost.RestoreReplace(ctx, key, ttl, data)

	}

	return
}

// Displays the help content
func showHelp() {
	fmt.Println(`
- Redis Key Migrator -
https://github.com/integrii/go-redis-migrator

Migrates all or some of the keys from a Redis host or cluster to a specified host or cluster.  Supports migrating TTLs.
go-redis-migrator can also be used to list the keys in a cluster.  Run with the -getKey=true and -sourceHosts= flags to do so.

You must run this program with an operation before it will do anything.

Flags:
  -getKeys=false: Fetches and prints keys from the source host.
  -copyData=false: Copies all keys in a list specified by -keyFile= to the destination cluster from the source cluster.
  -keyFile="": The file path which contains the list of keys to migrate.  One per line.
  -keyFilter="*": The filter for which keys to migrate.  Does not work when -keyFile is also specified.
  -destinationHosts="": A list of source cluster host:port servers seperated by spaces. Use a single host without a ',' if there is no cluster. EX) 127.0.0.1:6379,127.0.0.1:6380
  -sourceHosts="": A list of source cluster host:port servers seperated by commas. Use a single host without a ',' if there is no cluster. EX) 127.0.0.1:6379,127.0.0.1:6380
	`)

	os.Exit(0)
}
