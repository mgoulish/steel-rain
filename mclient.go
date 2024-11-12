package main

import (
	"bufio"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

// Used in Test 3

func send_message(conn net.Conn) {
	writer := bufio.NewWriter(conn)

	message := "Hello from client!\n"
	_, err := writer.WriteString(message)
	if err != nil {
		log.Println("Error writing to server:", err)
	}
	writer.Flush()
}

func send_messages_forever(conn net.Conn) {
	writer := bufio.NewWriter(conn)
	count := 0

	for {
		message := "Hello from client!\n"
		_, err := writer.WriteString(message)
		if err != nil {
			log.Println("Error writing to server:", err)
		}
		writer.Flush()

		count++
		if 0 == (count % 10000) {
			log.Println("Wrote: ", count)
		}
	}
}

func sendMessages(conn net.Conn) {
	writer := bufio.NewWriter(conn)
	ticker := time.Tick(5 * time.Millisecond)
	timeout := time.After(5 * time.Second)

	count := 0

	for {
		select {
		case <-ticker:
			message := "Hello from client!\n"
			_, err := writer.WriteString(message)
			if err != nil {
				log.Println("Error writing to server:", err)
				//log.Println ( "Wrote: ", count )
				return
			}
			writer.Flush()
			count++
			//if 0 == (count % 100) {
			//log.Println ( "Wrote: ", count )
			//}
		case <-timeout:
			log.Println("Timeout: wrote: ", count)
			return
		}
	}
}

func main() {
	port := os.Args[1]

	log_file := "mclient_" + port
	file, err := os.OpenFile(log_file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err) // If there's an error opening the file, log it and exit
	}
	defer file.Close()
	log.SetOutput(file)

	count := 0

	for {
		conn, err := net.Dial("tcp", "localhost:"+port)

		if err != nil {
			log.Printf("%d Error connecting to server: %v", count, err)
			time.Sleep(5 * time.Second) // In this case, long wait before retry.
			continue
		}

		// Only count it if we actually connect.
		count++ // Count every connectgion attempt

		// Randomly choose a behavior
		choice := rand.Intn(10)

		switch choice {
		case 0:
			log.Printf("%d immediate drop\n", count)
			conn.Close()
		case 1:
			log.Printf("%d send and drop\n", count)
			send_message(conn)
			conn.Close()
		default:
			log.Printf("%d drop after ack\n", count)
			reader := bufio.NewReader(conn)
			_, err = reader.ReadString('\n') // Read the ack message from the server
			if err != nil {
				log.Println("Error reading from server.")
			}
			conn.Close()
		}

		time.Sleep(200 * time.Millisecond) // Wait before reconnecting
	}
}
