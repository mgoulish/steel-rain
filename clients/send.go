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
                   message_length int,
                   id int,
                   throttle int,
                   addr string,
                   wg * sync.WaitGroup ) {
  defer wg.Done ( )
  msg := make([]byte, message_length) 
  for i := 0; i < message_length; i ++ {
    msg[i] = 'x'
  }

  fp ( os.Stdout, "Make session %d with addr %s\n", id, addr )
  // Open the session
  session, err := client.NewSession()
  check ( err, "Creating AMQP session" )

  // Send a message
  ctx := context.Background()
  // Create a sender
  sender, err := session.NewSender ( amqp.LinkTargetAddress ( addr ))
  check ( err, "Creating sender link" )

  ctx, cancel := context.WithTimeout ( ctx, 15 * time.Second )

  // Send message
  for i := 0; i < n_messages; i ++ {
    copy ( msg, fmt.Sprintf ( "Hello %d from session %d  ", i, id ) )
    err = sender.Send(ctx, amqp.NewMessage([]byte(msg)))
    if throttle > 0 {
      time.Sleep ( time.Duration(throttle) * time.Millisecond )
    }
  }
  fp ( os.Stdout, "Session %d sent %d messages.\n", id, n_messages )
  sender.Close(ctx)
  cancel()
}



func main() {

  host_p := flag.String ( "host", 
                          "127.0.0.1", 
                          "connect to this host" )

  port_p := flag.String ( "port", 
                          "5672",      
                          "connect to this port" )

  n_messages_p := flag.Int ( "n_messages", 
                             100,      
                             "how many messages to send to this port" )

  message_length_p := flag.Int ( "message_length", 
                                 100,      
                                 "how many messages to send to this port" )

  n_sessions_p := flag.Int ( "n_sessions",
                             1,
                             "how many sessions to run" )

  throttle_p := flag.Int ( "throttle",
                           0,
                           "milliseconds to pause between messages" )

  addrs_p := flag.String ( "addrs", 
                           "",
                           "list of addresses to distribute across sessions")

  flag.Parse()

  // Create client
  client, err := amqp.Dial ( "amqp:" + *host_p + *port_p,
                              amqp.ConnSASLAnonymous(),
                           )
  check ( err, "Dialing AMQP server" )
  defer client.Close()

  var wg sync.WaitGroup

  addrs := strings.Split ( * addrs_p, " " )
  n_addrs := len ( addrs )

  for i := 0; i < *n_sessions_p; i ++ {
    go run_session ( client, 
                     * n_messages_p, 
                     * message_length_p,
                     i,
                     * throttle_p,
                     addrs[i % n_addrs],
                     & wg )
    wg.Add ( 1 )
  }
  fp ( os.Stdout, "Launched %d sessions.\n", *n_sessions_p )

  wg.Wait ( )
}



