package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var fp = fmt.Fprintf

var listener_port int = 5672
var inter_router_listener_port int = 20000
var base_tcp_listener_port int = 5800
var base_tcp_connector_port int = 5900

func make_router_command(name string,
	router_commands map[string]*exec.Cmd,
	config_dir string) {
	ROUTER := "skrouterd"

	config_file := config_dir + "/" + name + ".conf"

	cmd := exec.Command(ROUTER, "--config", config_file)

	router_commands[name] = cmd
}

func start_router(name string, router_commands map[string]*exec.Cmd) {

	cmd := router_commands[name]
	//log.Printf ( "start_router: cmd: |%v|\n", cmd )

	file, err := os.OpenFile("router_"+name+"_output.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Redirect stdout and stderr to the file
	cmd.Stdout = file
	cmd.Stderr = file

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("start_router: %s started\n", name)
}

func stop_router(name string, cmd *exec.Cmd) error {
	pid := cmd.Process.Pid

	if err := cmd.Process.Kill(); err != nil {
		log.Println("stop_router: error on Kill: %v\n", err)
		return err
	}

	// Harvest process state, or it will defunctify
	var ws syscall.WaitStatus
	_, err := syscall.Wait4(pid, &ws, 0, nil)
	if err != nil {
		log.Println(err)
		log.Printf("stop_router: error on Wait4: %v\n", err)
		return err
	}
	log.Printf("stop_router: router %d exited with status %v\n", pid, ws)
	return nil
}

func is_running(pid int, cwd string) bool {

	err := syscall.Kill(pid, 0)
	// WRONG! Does not work for defunct processes
	if err != nil {
		log.Printf("is_running: err from Kill: |%v|\n", err)
		return false
	}

	// This script checks for defunct
	// I believe there are no more defuncts now with stop_router().
	cmd := exec.Command(cwd+"/check_status", strconv.Itoa(pid))

	output, err := cmd.Output()
	if err != nil {
		log.Printf("is_running: error running check_status\n")
		log.Fatal(err)
	}
	if 0 == len(output) {
		log.Printf("is_running: zero output from check_status\n")
		return false
	} else {
		if strings.Contains(string(output), "defunct") {
			log.Printf("is_running: %d is defunct\n", pid)
			return false
		}
	}
	//log.Printf ( "is_running: returning true: check_status output: |%s|\n", output )
	return true
}

// ---------------------------------------------------------
// All routers should be running when you call this.
// If any router is found to not be running, this exits.
// ---------------------------------------------------------
func check_routers(router_commands map[string]*exec.Cmd, cwd string) {
	for i := 0; i < len(router_commands); i++ {
		router_name := string(rune('A' + i))
		cmd := router_commands[router_name]
		if cmd.Process != nil {
			pid := cmd.Process.Pid
			if is_running(pid, cwd) {
			} else {
				log.Printf("check_routers: Router %s, pid %d is not running.\n", router_name, pid)
				os.Exit(1)
			}
		}
	}
	log.Printf("check_routers: all routers are running\n")
}

func bounce_routers(wg *sync.WaitGroup,
	network_size int,
	router_commands map[string]*exec.Cmd,
	config_dir string,
	cwd string) {
	defer wg.Done()

	count := 0
	for {
		check_routers(router_commands, cwd)
		count++
		n := rand.Intn(network_size)
		name := string(rune('A' + n))
		log.Printf("Bounce %d ------------------------------------\n", count)

		make_config_file(n,
			network_size,
			config_dir,
			nil,
			nil)

		cmd := router_commands[name]

		stop_router(name, cmd)
		time.Sleep(time.Duration(10+rand.Intn(15)) * time.Second)
		cmd.Process = nil
		start_router(name, router_commands)
		//time.Sleep ( 15 * time.Second )
	}
}

func make_dir(dir string) {
	absPath, err := filepath.Abs(dir)
	if err != nil {
		log.Printf("Error getting absolute path: %v\n", err)
		return
	}

	err = os.MkdirAll(absPath, 0755)
	if err != nil {
		log.Printf("Error creating directory: %v\n", err)
		return
	}

	log.Printf("make_dir: directory created at: %s\n", absPath)
}

func make_config_file(router_number int,
	network_size int,
	config_dir string,
	listener_to_port_map map[string]int,
	connector_to_port_map map[string]int) {

	router_id := string(rune('A' + router_number))
	log.Printf("make_config_file: router %s\n", router_id)
	config_file_path := config_dir + "/" + router_id + ".conf"
	config_file, e := os.OpenFile(config_file_path,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0644)
	if e != nil {
		log.Printf("Error opening file: %v\n", e)
		return
	}
	defer config_file.Close()
	w := bufio.NewWriter(config_file)

	// router block -----------------------------------------
	fp(w, "router {\n")
	fp(w, "  id:            %s\n", router_id)
	fp(w, "  mode:          interior\n")
	fp(w, "  workerThreads: 2\n")
	fp(w, "}\n\n")

	// AMQP listener block ----------------------------------
	fp(w, "listener {\n")
	fp(w, "  port: %d\n", listener_port+router_number)
	fp(w, "  role: normal\n")
	fp(w, "  host: 0.0.0.0\n")
	fp(w, "}\n\n")
	w.Flush()

	// Inter-router listener --------------------------------
	// The first router doesn't have one, because no other
	// router will connect to him.
	if router_number > 0 {
		fp(w, "listener {\n")
		fp(w, "  name: inter-router-listener-%s\n", router_id)
		fp(w, "  port: %d\n", inter_router_listener_port+router_number-1)
		fp(w, "  role: inter-router\n")
		fp(w, "  idleTimeoutSeconds: 120\n")
		fp(w, "  saslMechanisms: ANONYMOUS\n")
		fp(w, "  host: 0.0.0.0\n")
		fp(w, "  authenticatePeer: no\n")
		fp(w, "}\n\n")
		w.Flush()
	}

	// TCP listeners ---------------------------------------
	// The network will have a total of N * 3 possible TCP
	// addresses. Each router will get 3 TCP listeners with
	// addresses chosen at random
	for i := 0; i < 3; i++ {
		addr := fmt.Sprintf("addr_%d", rand.Intn(network_size*3))
		fp(w, "tcpListener {\n")
		tcp_listener_name := fmt.Sprintf("tcp-listener-%s-%d", router_id, i)
		fp(w, "  name:  %s\n", tcp_listener_name)
		port := base_tcp_listener_port + (router_number * 3) + i
		fp(w, "  port: %d\n", port)
		fp(w, "  host: 0.0.0.0\n")
		fp(w, "  address: %s\n", addr)
		fp(w, "  siteId: my-site\n")
		fp(w, "}\n\n")
		log.Printf("    name %s port %d addr %s\n", tcp_listener_name, port, addr)
		w.Flush()
		if listener_to_port_map != nil {
			listener_to_port_map[tcp_listener_name] = port
		}
	}

	// TCP connector ---------------------------------------
	// The network will have a total of N * 3 possible TCP
	// addresses. Each router will get 3 TCP listeners with
	// addresses chose at random
	for i := 0; i < 3; i++ {
		// addr := fmt.Sprintf ( "addr_%d", rand.Intn(network_size * 3) )
		// TEMP: Try this: only use network_size addresses, to get more
		// messages to actually fly.
		addr := fmt.Sprintf("addr_%d", rand.Intn(network_size))
		fp(w, "tcpConnector {\n")
		tcp_connector_name := fmt.Sprintf("tcp-connector-%s-%d", router_id, i)
		fp(w, "  name: %s\n", tcp_connector_name)
		port := base_tcp_connector_port + (router_number * 3) + i
		fp(w, "  port: %d\n", port)
		fp(w, "  host: 127.0.0.1\n")
		fp(w, "  address: %s\n", addr)
		fp(w, "}\n\n")
		w.Flush()
		if connector_to_port_map != nil {
			connector_to_port_map[tcp_connector_name] = port
		}
	}

	// Inter-Router Connectors -----------------------------
	// To make a completely-connected graph, each router
	// connects to all higher-number routers.
	for j := router_number + 1; j < network_size; j++ {
		connect_to_id := string(rune('A' + j))
		// Numbering of inter router listener ports
		// starts at outer B, not A.
		// So when router_number == 0 at Router A, we want the
		// connect-to port to be 20,000. (The port for B.)
		connect_to_port := inter_router_listener_port + j - 1
		fp(w, "connector {\n")
		fp(w, "  name: %s-connector-to-%s\n", router_id, connect_to_id)
		fp(w, "  port: %d\n", connect_to_port)
		fp(w, "  role: inter-router\n")
		fp(w, "  stripAnnotations: no\n")
		fp(w, "  idleTimeoutSeconds: 120\n")
		fp(w, "  saslMechanisms: ANONYMOUS\n")
		fp(w, "  host: 127.0.0.1\n")
		fp(w, "}\n\n")
		w.Flush()
	}
}

func start_servers(cwd string) {
	for port := base_tcp_connector_port; port < base_tcp_connector_port+15; // TEMP
	port++ {
		cmd := exec.Command(cwd+"/r_server",
			strconv.Itoa(port))
		err := cmd.Start()
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("start_servers: started server on port %d\n", port)
		}
		time.Sleep(300 * time.Millisecond) // No need to randomize
	}
}

func start_clients(cwd string) {
	for port := base_tcp_listener_port; port < base_tcp_listener_port+15; // TEMP
	port++ {
		cmd := exec.Command(cwd+"/r_client",
			strconv.Itoa(port))
		err := cmd.Start()
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("start_clients: started client on port %d\n", port)
		}
		time.Sleep(300 * time.Millisecond) // No need to randomize
	}
}

func bounce_listeners(network_size int,
	listener_to_port_map map[string]int,
	skmanage string) {
	listeners_per_router := 3

	count := 0

	for {
		router_n := rand.Intn(network_size)
		random_router_id := string(rune('A' + router_n))
		random_listener_id := rand.Intn(listeners_per_router)
		listener_name := fmt.Sprintf("tcp-listener-%s-%d", random_router_id, random_listener_id)

		cmd := exec.Command(skmanage,
			"delete",
			"--type=tcpListener",
			fmt.Sprintf("--name=%s", listener_name))
		count++
		log.Printf("bounce_listeners %d: delete %s \n", count, listener_name)
		cmd.Run()
		time.Sleep(time.Duration(3+rand.Intn(5)) * time.Second)
		port := listener_to_port_map[listener_name]
		cmd = exec.Command(skmanage,
			"create",
			"--type=tcpListener",
			fmt.Sprintf("--name=%s", listener_name),
			"host=0.0.0.0",
			fmt.Sprintf("port==%d", port),
			fmt.Sprintf("address=addr_%d", rand.Intn(network_size*3)))
		log.Printf("bounce_listeners %d: create %s on port %d \n", count, listener_name, port)
		cmd.Run()
		time.Sleep(time.Duration(3+rand.Intn(5)) * time.Second)
	}
}

func management_commands(network_size int, skmanage string) {

	var commands []*exec.Cmd

	// command : connector query --------------------------------------
	port := 5672 + rand.Intn(network_size)
	cmd := exec.Command(skmanage,
		"QUERY",
		"--type=tcpConnector",
		"--bus",
		fmt.Sprintf("amqp://0.0.0.0:%d", port),
		"--indent=2",
		"--timeout",
		"60.0")
	commands = append(commands, cmd)

	// command : get logs --------------------------------------
	port = 5672 + rand.Intn(network_size)
	cmd = exec.Command(skmanage,
		"get-log",
		"--bus",
		fmt.Sprintf("amqp://0.0.0.0:%d", port),
		"--indent=2",
		"--timeout",
		"60.0")
	commands = append(commands, cmd)

	// command : query autolink --------------------------------------
	port = 5672 + rand.Intn(network_size)
	cmd = exec.Command(skmanage,
		"QUERY",
		"--type=io.skupper.router.router.config.autoLink",
		"--bus",
		fmt.Sprintf("amqp://0.0.0.0:%d", port),
		"--indent=2",
		"--timeout",
		"60.0")
	commands = append(commands, cmd)

	// command : query metrics --------------------------------------
	port = 5672 + rand.Intn(network_size)
	cmd = exec.Command(skmanage,
		"QUERY",
		"--type=io.skupper.router.routerMetrics",
		"--bus",
		fmt.Sprintf("amqp://0.0.0.0:%d", port),
		"--indent=2",
		"--timeout",
		"60.0")
	commands = append(commands, cmd)

	for {
		time.Sleep(time.Duration(3+rand.Intn(5)) * time.Second)
		cmd := commands[rand.Intn(len(commands))]
		fmt.Printf("MANAGEMENT_COMMAND: |%v|\n", cmd)
		cmd.Stdout = nil
		cmd.Process = nil
		cmd.ProcessState = nil
		//out, err := cmd.Output()
		_, err := cmd.Output()
		if err != nil {
			//log.Fatal(err)
			log.Println("management_commands: error getting mgmt command output:", err)
		} else {
			fmt.Printf("management_commands: received mgmt command output\n")
		}
		//fmt.Printf ( "  mgmt command output: |%s|\n", out)
	}
}

func bounce_connectors(network_size int,
	connector_to_port_map map[string]int,
	skmanage string) {
	connectors_per_router := 3 // TEMP

	log.Printf("bounce_connectors : using path %s\n", skmanage)

	count := 0
	for {
		router_n := rand.Intn(network_size)
		random_router_id := string(rune('A' + router_n))
		random_connector_id := rand.Intn(connectors_per_router)
		connector_name := fmt.Sprintf("tcp-connector-%s-%d", random_router_id, random_connector_id)

		cmd := exec.Command(skmanage,
			"delete",
			"--type=tcpConnector",
			fmt.Sprintf("--name=%s", connector_name))
		count++
		log.Printf("bounce_connectors %d: delete %s\n", count, connector_name)
		cmd.Run()
		time.Sleep(time.Duration(3+rand.Intn(5)) * time.Second)
		port := connector_to_port_map[connector_name]
		cmd = exec.Command(skmanage,
			"create",
			"--type=tcpConnector",
			fmt.Sprintf("--name=%s", connector_name),
			"host=0.0.0.0",
			fmt.Sprintf("port==%d", port),
			fmt.Sprintf("address=addr_%d", rand.Intn(network_size*3)))
		log.Printf("bounce_connectors %d: create %s on port %d\n", count, connector_name, port)
		cmd.Run()
		time.Sleep(time.Duration(3+rand.Intn(5)) * time.Second)
	}
}

func main() {

	skmanage_flag := flag.String("skmanage",
		"skmanage",
		"path to skmanage executable")

	flag.Parse()
	fmt.Println("skmanage:", *skmanage_flag)

	rand.Seed(time.Now().UnixNano())
	network_size := 5
	var wg sync.WaitGroup
	listener_to_port_map := make(map[string]int)
	connector_to_port_map := make(map[string]int)

	config_dir := fmt.Sprintf("./cc_%d_configs", network_size)

	router_commands := make(map[string]*exec.Cmd)
	for i := 0; i < network_size; i++ {
		router_id := string(rune('A' + i))
		make_router_command(router_id,
			router_commands,
			config_dir)
	}

	make_dir(config_dir)
	for i := 0; i < network_size; i++ {
		make_config_file(i,
			network_size,
			config_dir,
			listener_to_port_map,
			connector_to_port_map)
	}

	log.Printf("main: starting routers\n")
	for i := 0; i < network_size; i++ {
		router_id := string(rune('A' + i))
		start_router(router_id, router_commands)
	}
	log.Printf("Pause 5 seconds.\n")
	time.Sleep(5 * time.Second) // No need to randomize

	/*
	  for {
	    log.Println ( "main: TEMP sleeping after starting routers\n" )
	    time.Sleep ( 10 * time.Second )  // No need to randomize
	  }
	*/

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("main: CWD == |%s|\n", cwd)

	go management_commands(network_size, *skmanage_flag)

	log.Printf("main: starting TCP servers\n")
	start_servers(cwd)
	time.Sleep(5 * time.Second) // No need to randomize

	log.Printf("main: starting TCP clients\n")
	start_clients(cwd)

	log.Printf("main: sleeping 10 seconds before starting to bounce routers.\n")
	time.Sleep(10 * time.Second) // No need to randomize
	go bounce_routers(&wg,
		network_size,
		router_commands,
		config_dir,
		cwd)
	wg.Add(1)

	time.Sleep(2 * time.Second) // No need to randomize
	log.Printf("main: start bouncing listeners.\n")
	go bounce_listeners(network_size, listener_to_port_map, *skmanage_flag)

	time.Sleep(2 * time.Second) // No need to randomize
	log.Printf("main: start bouncing connectors.\n")
	go bounce_connectors(network_size, connector_to_port_map, *skmanage_flag)

	wg.Wait()

	for {
		log.Println("main: sleeping\n")
		time.Sleep(10 * time.Second) // No need to randomize
	}

	log.Printf("main: exiting\n")
}
