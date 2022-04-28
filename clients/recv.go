package main

import (
        "context"
        "flag"
        "fmt"
        "log"
        "os"
        "strings"
        "sync"
        "time"
        "github.com/interconnectedcloud/go-amqp"
)



var fp = fmt.Fprintf



func check ( err error, msg string ) {
  if err != nil {
    log.Fatal ( msg + ": ", err )
  }
}



func run_session ( client * amqp.Client, 
                   n_messages int,
                   id int,
                   addr string,
                   wg * sync.WaitGroup ) {

  defer wg.Done ( )

  fp ( os.Stdout, "Receiver starting session %d with addr %s\n", id, addr )

  // Open the session
  session, err := client.NewSession()
  check ( err, "Creating AMQP session" )

  // read messages
  ctx := context.Background ( )

  // Create a receiver
  receiver, err := session.NewReceiver (
                     amqp.LinkSourceAddress ( addr ),
                     amqp.LinkCredit ( 10 ),
                   )
  check ( err, "Creating receiver link" )

  defer func ( ) {
    ctx, cancel := context.WithTimeout ( ctx, 1 * time.Second )
    receiver.Close ( ctx )
    cancel ( )
  } ( )

  message_count := 0
  for {
    // Receive next message
    msg, err := receiver.Receive(ctx)
    check ( err, "Reading message from AMQP" )
    msg.Accept()

    // msg.GetData() If you want to print it out...

    message_count ++
    if 0 == message_count % 100 {
      fp ( os.Stdout, 
           "Session %d addr %s has received %d messages.\n", 
           id, 
           addr,
           message_count )
    }

    if message_count >= n_messages {
      break
    }
  }

}



func main() {

  // Set up command line flags.
  host_p := flag.String ( "host", 
                          "127.0.0.1", 
                          "connect to this host")

  port_p := flag.String ( "port", 
                          "5672",      
                          "connect to this port")

  n_messages_p := flag.Int ( "n_messages", 
                             100,      
                             "receive this many messages")

  n_sessions_p := flag.Int ( "n_sessions",
                             1,
                             "how many sessions to run" )

  addrs_p := flag.String ( "addrs",
                           "",
                           "list of addresses to distribute across sessions")

  flag.Parse()


  var wg sync.WaitGroup

  addrs := strings.Split ( * addrs_p, " " )
  n_addrs := len(addrs)

  // Create the client.
  client, err := amqp.Dial ( "amqp:" + *host_p + *port_p,
                              amqp.ConnSASLAnonymous(),
                           )
  check ( err, "Dialing AMQP server" )
  defer client.Close()

  // Start the sessions.
  for i := 0; i < *n_sessions_p; i ++ {
    go run_session ( client, 
                     * n_messages_p,
                     i,
                     addrs[i % n_addrs],
                     & wg )
    wg.Add ( 1 )
  }

  wg.Wait ( )
}




