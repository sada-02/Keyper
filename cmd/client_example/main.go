package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/sada-02/keyper/client"
)

func main() {
	var nodes string
	flag.StringVar(&nodes, "nodes", "http://127.0.0.1:8080", "comma-separated list of node http addresses")
	flag.Parse()

	addrs := []string{}
	for _, x := range split(nodes) {
		if x != "" {
			addrs = append(addrs, x)
		}
	}
	cl := client.New(addrs)

	// simple interactive demo if args present
	if len(flag.Args()) >= 1 {
		switch flag.Args()[0] {
		case "put":
			if len(flag.Args()) < 3 {
				fmt.Println("usage: client_example put <key> <value>")
				os.Exit(2)
			}
			if err := cl.Put(flag.Args()[1], []byte(flag.Args()[2])); err != nil {
				fmt.Printf("put error: %v\n", err)
				os.Exit(1)
			}
			fmt.Println("ok")
		case "get":
			if len(flag.Args()) < 2 {
				fmt.Println("usage: client_example get <key>")
				os.Exit(2)
			}
			v, err := cl.Get(flag.Args()[1])
			if err != nil {
				fmt.Printf("get error: %v\n", err)
				os.Exit(1)
			}
			fmt.Println(string(v))
		case "del":
			if len(flag.Args()) < 2 {
				fmt.Println("usage: client_example del <key>")
				os.Exit(2)
			}
			if err := cl.Delete(flag.Args()[1]); err != nil {
				fmt.Printf("delete error: %v\n", err)
				os.Exit(1)
			}
			fmt.Println("deleted")
		default:
			fmt.Println("commands: put|get|del")
		}
		return
	}

	// interactive quick demo
	fmt.Println("Client demo. Try: client_example put foo bar")
}

// split splits comma-separated node list, trims spaces, and normalizes to http:// prefix if needed.
func split(s string) []string {
	out := []string{}
	for _, part := range strings.Split(s, ",") {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		// ensure scheme present; client.New also tolerates no-scheme, but keep consistent
		if !strings.HasPrefix(p, "http://") && !strings.HasPrefix(p, "https://") {
			p = "http://" + p
		}
		out = append(out, strings.TrimRight(p, "/"))
	}
	return out
}
