/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <proton/connection.h>
#include <proton/condition.h>
#include <proton/delivery.h>
#include <proton/link.h>
#include <proton/message.h>
#include <proton/proactor.h>
#include <proton/session.h>
#include <proton/transport.h>

#include <stdio.h>
#include <stdlib.h>

#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>


int total_acknowledged = 0;



static
double
get_timestamp ( void )
{
  struct timeval t;
  gettimeofday ( & t, 0 );
  return t.tv_sec + ((double) t.tv_usec) / 1000000.0;
}



typedef struct app_data_t {
  char * host, 
       * port,
       * address,
       * container_id;

  int message_count;

  pn_proactor_t *proactor;
  pn_message_t *message;
  pn_rwbytes_t message_buffer;
  int sent;
  int acknowledged;

  int len;
  char * content;
} app_data_t;

static int exit_code = 0;

static void check_condition(pn_event_t *e, pn_condition_t *cond) {
  if (pn_condition_is_set(cond)) {
    fprintf(stderr, "%s: %s: %s\n", pn_event_type_name(pn_event_type(e)),
            pn_condition_get_name(cond), pn_condition_get_description(cond));
    pn_connection_close(pn_event_connection(e));
    exit_code = 1;
  }
}

static void send_message(app_data_t* app, pn_link_t *sender, char * content, int len) 
{
  pn_data_t* body;
  pn_message_clear(app->message);
  body = pn_message_body(app->message);
  pn_data_put_int(pn_message_id(app->message), app->sent);
  pn_data_enter(body);
  pn_data_put_string(body, pn_bytes(len, content));
  pn_data_exit(body);


  if (pn_message_send(app->message, sender, &app->message_buffer) < 0) {
    fprintf(stderr, "error sending message: %s\n", 
            pn_error_text(pn_message_error(app->message)));
    exit(1);
  }
}



/* Returns true to continue, false if finished */
static bool handle(app_data_t* app, pn_event_t* event, char * content, int len) {
  switch (pn_event_type(event)) {

   case PN_CONNECTION_INIT: {
     pn_connection_t* c = pn_event_connection(event);
     pn_session_t* s = pn_session(pn_event_connection(event));
     pn_connection_set_container(c, app->container_id);
     pn_connection_open(c);
     pn_session_open(s);
     {
     pn_link_t* l = pn_sender(s, "my_sender");
     pn_terminus_set_address(pn_link_target(l), app->address);
     pn_link_open(l);

     break;
     }
   }

   case PN_LINK_FLOW: {
     /* The peer has given us some credit, now we can send messages */
     pn_link_t *sender = pn_event_link(event);
     while (pn_link_credit(sender) > 0 && app->sent < app->message_count) {
       ++app->sent;
       /* Use sent counter as unique delivery tag. */
       pn_delivery(sender, pn_dtag((const char *)&app->sent, sizeof(app->sent)));
       send_message(app, sender, content, len);
     }
     break;
   }

   case PN_DELIVERY: {
     /* We received acknowledgement from the peer that a message was delivered. */
     pn_delivery_t* d = pn_event_delivery(event);
     if (pn_delivery_remote_state(d) == PN_ACCEPTED) {

       ++ total_acknowledged;
       /*fprintf ( stderr, "MDEBUG total_acknowledged %d\n" , total_acknowledged); */
       if (++app->acknowledged >= app->message_count) {
         printf("%d messages sent and acknowledged\n", app->acknowledged);
         pn_connection_close(pn_event_connection(event));
         /* Continue handling events till we receive TRANSPORT_CLOSED */
       }
     } else {
       fprintf(stderr, "unexpected delivery state %d\n", (int)pn_delivery_remote_state(d));
       pn_connection_close(pn_event_connection(event));
       exit_code=1;
     }
     break;
   }

   case PN_TRANSPORT_CLOSED:
    check_condition(event, pn_transport_condition(pn_event_transport(event)));
    break;

   case PN_CONNECTION_REMOTE_CLOSE:
    check_condition(event, pn_connection_remote_condition(pn_event_connection(event)));
    pn_connection_close(pn_event_connection(event));
    break;

   case PN_SESSION_REMOTE_CLOSE:
    check_condition(event, pn_session_remote_condition(pn_event_session(event)));
    pn_connection_close(pn_event_connection(event));
    break;

   case PN_LINK_REMOTE_CLOSE:
   case PN_LINK_REMOTE_DETACH:
    check_condition(event, pn_link_remote_condition(pn_event_link(event)));
    pn_connection_close(pn_event_connection(event));
    break;

   case PN_PROACTOR_INACTIVE:
    return false;

   default: break;
  }
  return true;
}

void run(app_data_t *app) {
  /* Loop and handle events */
  do {
    pn_event_batch_t *events = pn_proactor_wait(app->proactor);
    pn_event_t *e;
    for (e = pn_event_batch_next(events); e; e = pn_event_batch_next(events)) {
      if (!handle(app, e, app->content, app->len)) {
        return;
      }
    }
    pn_proactor_done(app->proactor, events);
  } while(true);
}



void
parse_args ( int argc, char **argv, struct app_data_t * app )
{
  app->container_id = (char *) malloc(20);
  sprintf ( app->container_id, "send_%d", getpid() );

  // Defaults.
  app->host          = "";
  app->port          = "amqp";
  app->address       = "examples";
  app->message_count = 100000;
  app->len           = 100;

  // Read command line.
  for ( int i = 1; i < argc; ++ i ) 
  {
    if ( ! strcmp("host", argv[i]) )
    {
      app->host = argv[i+1];
      i ++;
    }
    else
    if ( ! strcmp("port", argv[i]) )
    {
      app->port = argv[i+1];
      i ++;
    }
    else
    if ( ! strcmp("address", argv[i]) )
    {
      app->address = argv[i+1];
      i ++;
    }
    else
    if ( ! strcmp("message_count", argv[i]) )
    {
      app->message_count = atoi(argv[i+1]);
      i ++;
    }
    else
    if ( ! strcmp("len", argv[i]) )
    {
      app->len = atoi(argv[i+1]);
      i ++;
    }
    else
    {
      fprintf(stderr, "Unknown option: |%s|\n", argv[i] );
      exit(1);
    }
  }
}



int main(int argc, char **argv) {
  double start_time;

  struct app_data_t app = {0};
  char addr[PN_MAX_ADDR];

  parse_args ( argc, argv, & app );

  app.content = (char *) malloc(app.len);
  memset ( app.content, 'x', app.len );
  app.message = pn_message();

  start_time = get_timestamp ( );

  app.proactor = pn_proactor();
  pn_proactor_addr(addr, sizeof(addr), app.host, app.port);
  pn_proactor_connect2(app.proactor, NULL, NULL, addr);
  run(&app);
  pn_proactor_free(app.proactor);
  free(app.message_buffer.start);
  pn_message_free(app.message);

  fprintf ( stderr, "start time was %.6f\n", start_time );
  return exit_code;
}
