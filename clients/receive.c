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

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>


#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int messages_received = 0;
int bytes_received    = 0;

static
double
get_timestamp ( void )
{
  struct timeval t;
  gettimeofday ( & t, 0 );
  return t.tv_sec + ((double) t.tv_usec) / 1000000.0;
}


double start_time;
double stop_time;


typedef struct app_data_t {
  char * host,
       * port,
       * address,
       * container_id;

  int message_count;

  pn_proactor_t *proactor;
  pn_listener_t *listener;
  pn_rwbytes_t msgin, msgout;   /* Buffers for incoming/outgoing messages */

  /* Sender values */
  int sent;
  int acknowledged;
  pn_link_t *sender;

  /* Receiver values */
  int received;
  int report_frequency;
} app_data_t;


static const int BATCH = 1000; /* Batch size for unlimited receive */

static int exit_code = 0;

static void check_condition(pn_event_t *e, pn_condition_t *cond) {
  if (pn_condition_is_set(cond)) {
    fprintf(stderr, "%s: %s: %s\n", pn_event_type_name(pn_event_type(e)),
            pn_condition_get_name(cond), pn_condition_get_description(cond));
    pn_connection_close(pn_event_connection(e));
    exit_code = 1;
  }
}

void
report ( app_data_t * app )
{
  double duration, Bps, Mps;
  stop_time = get_timestamp ( );
  printf ( "--------------------------------------------\n" );
  printf("%d messages received\n", app->received);
  printf("%d  bytes received\n", bytes_received);
  duration = stop_time - start_time;
  fprintf ( stdout, "%lf %lf %lf\n", start_time, stop_time, duration);
  Bps = (double) bytes_received / duration;
  Mps = (double) app->received  / duration;

  fprintf ( stdout, "bps = %.0f\n", 8 * Bps );
  fprintf ( stdout, "Mps = %.0f\n", Mps );
  fflush(stdout);
}

static void decode_message(pn_rwbytes_t data) {
  pn_message_t *m = pn_message();
  int err = pn_message_decode(m, data.start, data.size);
  if (!err) {
    pn_string_t *s = pn_string(NULL);
    pn_inspect(pn_message_body(m), s);
    bytes_received += strlen(pn_string_get(s));
    pn_free(s);
    pn_message_free(m);
    free(data.start);
  } else {
    fprintf(stderr, "decode_message: %s\n", pn_code(err));
    exit_code = 1;
  }
}


/* Return true to continue, false to exit */
static bool handle(app_data_t* app, pn_event_t* event) {
  switch (pn_event_type(event)) {

   case PN_CONNECTION_INIT: {
     pn_connection_t* c = pn_event_connection(event);
     pn_session_t* s = pn_session(c);
     pn_connection_set_container(c, app->container_id);
     pn_connection_open(c);
     pn_session_open(s);
     {
     pn_link_t* l = pn_receiver(s, "my_receiver");
     pn_terminus_set_address(pn_link_source(l), app->address);
     pn_link_open(l);
     /* cannot receive without granting credit: */
     pn_link_flow(l, app->message_count ? app->message_count : BATCH);
     }
   } break;

   case PN_DELIVERY: {
     /* A message (or part of a message) has been received */
     pn_delivery_t *d = pn_event_delivery(event);
     if (pn_delivery_readable(d)) {
       pn_link_t *l = pn_delivery_link(d);
       size_t size = pn_delivery_pending(d);
       pn_rwbytes_t* m = &app->msgin; /* Append data to incoming message buffer */
       int recv;
       size_t oldsize = m->size;
       m->size += size;
       m->start = (char*)realloc(m->start, m->size);
       recv = pn_link_recv(l, m->start + oldsize, m->size);
       if (recv == PN_ABORTED) {
         printf("Message aborted\n");
         m->size = 0;           /* Forget the data we accumulated */
         pn_delivery_settle(d); /* Free the delivery so we can receive the next message */
         pn_link_flow(l, 1);    /* Replace credit for aborted message */
       } else if (recv < 0 && recv != PN_EOS) {        /* Unexpected error */
         pn_condition_format(pn_link_condition(l), "broker", "PN_DELIVERY error: %s", pn_code(recv));
         pn_link_close(l);               /* Unexpected error, close the link */
       } else if (!pn_delivery_partial(d)) { /* Message is complete */

         /* We got a message! */
         if ( app->received == 0 ) 
         {
           start_time = get_timestamp ( );
         }

         decode_message(*m);
         *m = pn_rwbytes_null;  /* Reset the buffer for the next message*/
         /* Accept the delivery */
         pn_delivery_update(d, PN_ACCEPTED);
         pn_delivery_settle(d);  /* settle and free d */

         app->received ++;
         if ( ! (app->received % app->report_frequency) )
         {
           report ( app );
         }

         if (app->message_count == 0)
         {
           /* receive forever - see if more credit is needed */
           if (pn_link_credit(l) < BATCH/2) 
           {
             pn_link_flow(l, BATCH - pn_link_credit(l));
           }
         }
         else
         if (app->received >= app->message_count)
         {
           pn_session_t *ssn = pn_link_session(l);
           report ( app );
           pn_link_close(l);
           pn_session_close(ssn);
           pn_connection_close(pn_session_connection(ssn));
         }
       }
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
    break;

   default:
    break;
  }
    return true;
}

void run(app_data_t *app) {
  /* Loop and handle events */
  do {
    pn_event_batch_t *events = pn_proactor_wait(app->proactor);
    pn_event_t *e;
    for (e = pn_event_batch_next(events); e; e = pn_event_batch_next(events)) {
      if (!handle(app, e) || exit_code != 0) {
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
  app->host             = "";
  app->port             = "amqp";
  app->address          = "examples";
  app->message_count    = 100000;
  app->report_frequency = 1000000;

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
    if ( ! strcmp("report", argv[i]) )
    {
      app->report_frequency = atoi(argv[i+1]);
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
  struct app_data_t app = {0};
  char addr[PN_MAX_ADDR];

  parse_args ( argc, argv, & app );
  fprintf ( stdout, "message_count: %d\n", app.message_count );
  fprintf ( stdout, "report:        %d\n", app.report_frequency );
  fprintf ( stdout, "address:       %s\n", app.address );
  fprintf ( stdout, "port:          %s\n", app.port );
  fprintf ( stdout, "host:          %s\n", app.host );
  fflush  ( stdout );


  /* Create the proactor and connect */
  app.proactor = pn_proactor();
  pn_proactor_addr(addr, sizeof(addr), app.host, app.port);
  pn_proactor_connect2(app.proactor, NULL, NULL, addr);
  run(&app);
  pn_proactor_free(app.proactor);
  return exit_code;
}
