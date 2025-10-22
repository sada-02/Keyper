package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/sada-02/keyper/client"
	"github.com/sada-02/keyper/shard"
)

func main() {
	var nodesStr string
	var nkeys int
	var replicas int
	flag.StringVar(&nodesStr, "nodes", "http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082", "comma-separated node http addresses")
	flag.IntVar(&nkeys, "nkeys", 1000, "number of keys to PUT and GET")
	flag.IntVar(&replicas, "replicas", 150, "virtual node replicas for ring")
	flag.Parse()

	// parse nodes list (very simple)
	nodes := []string{}
	for _, p := range split(nodesStr) {
		nodes = append(nodes, p)
	}

	if len(nodes) == 0 {
		log.Fatal("no nodes supplied")
	}

	sc := client.NewShardedClient(nodes, replicas)
	// Also build a local ring to map keys -> nodes for reporting.
	r := shard.NewRing(replicas)
	for _, n := range nodes {
		r.AddNode(n)
	}

	fmt.Printf("Running sharded test: %d keys across %d nodes (replicas=%d)\n", nkeys, len(nodes), replicas)

	dist := map[string]int{}
	start := time.Now()
	for i := 0; i < nkeys; i++ {
		key := fmt.Sprintf("k-%d", i)
		val := []byte(fmt.Sprintf("v-%d", i))
		if err := sc.Put(key, val); err != nil {
			log.Fatalf("Put failed for %s: %v", key, err)
		}
		got, err := sc.Get(key)
		if err != nil {
			log.Fatalf("Get failed for %s: %v", key, err)
		}
		if string(got) != string(val) {
			log.Fatalf("Value mismatch for %s: got=%s want=%s", key, string(got), string(val))
		}
		node, _ := r.GetNode(key)
		dist[node]++
		if i%200 == 0 && i > 0 {
			fmt.Printf("...progress %d/%d\n", i, nkeys)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("All %d keys written+verified in %s\n", nkeys, elapsed)
	fmt.Println("Key distribution per node (approx):")
	for _, n := range r.Nodes() {
		fmt.Printf("  %s -> %d\n", n, dist[n])
	}
	fmt.Println("sharded_test completed successfully.")
}

// split helper
func split(s string) []string {
	out := []string{}
	cur := ""
	for _, ch := range s {
		if ch == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(ch)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
