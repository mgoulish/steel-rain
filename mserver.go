package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func handleConnection(conn net.Conn, count int) {
	defer conn.Close()

	// Send a welcome message to the client.
	// This tells the client that the connection is live.
	welcomeMessage := "Welcome to the server!\n"
	_, err := conn.Write([]byte(welcomeMessage))
	if err != nil {
		log.Println("Error sending welcome message:", err)
		return
	}

	// Don't bother trying to read messages
	// from the client. Just close it.
	return

	reader := bufio.NewReader(conn)

	n_messages := 0
	for {
		//message, err := reader.ReadString('\n')
		_, err := reader.ReadString('\n')
		if err != nil {
			//log.Println("Error reading:", err)
			break
		}
		//fmt.Print(message)
		n_messages++
	}
	fmt.Println("Client", count, "sent ", n_messages)
}

func main() {
	port := os.Args[1]
	fmt.Println("SERVER for port ", port)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("Error starting server:", err)
	}
	defer listener.Close()

        cwd, err := os.Getwd()
        if err != nil {
                log.Fatal(err)
        }

	log_file := cwd + "/server_output/mserver_" + port

	file, err := os.OpenFile(log_file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	log.SetOutput(file)

	log.Printf("Server listening on %s\n", port)

	client_count := 0
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		//log.Println("New client connected")
		go handleConnection(conn, client_count)
		client_count++
		if 0 == (client_count % 1000) {
			log.Println(client_count)
		}
	}
}
