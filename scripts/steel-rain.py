#! /usr/bin/python

import os
import sys
import subprocess
import shutil
import time
import datetime
import socket
import random



context = { "routers"        : {},
            "receivers"      : {},
            "senders"        : {},
            "addresses"      : [],
            "clients_list"   : [],
            "sender_count"   : 0,
            "receiver_count" : 0 }




default_sender_args = { 'router'     : 'A', \
                        'n_senders'  :  10, \
                        'n_messages' :  1000000000 }

default_receiver_args = { 'router'       : 'B',         \
                          'n_receivers'  :  10,         \
                          'n_messages'   :  1000000000, \
                          'report_freq'  :  1000000 }



def find_open_port():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("",0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        return port



def find_router ( ) :
    context['router'] = shutil.which ( 'qdrouterd' )



def check_env ( ):
    dispatch_install    = os.environ.get('STEEL_RAIN_DISPATCH_INSTALL')
    if dispatch_install == None:
      print ( "please set STEEL_RAIN_DISPATCH_INSTALL env var.")
      sys.exit(1)

    proton_root    = os.environ.get('STEEL_RAIN_PROTON_ROOT')
    if proton_root == None:
      print ( "please set STEEL_RAIN_PROTON_ROOT env var.")
      sys.exit(1)

    proton_install = os.environ.get('STEEL_RAIN_PROTON_INSTALL')
    if proton_install == None:
      print ( "please set STEEL_RAIN_PROTON_INSTALL env var.")
      sys.exit(1)


    # Make sure we have the executables we need.
    if not os.path.isfile ( '../clients/send' ) :
      print ( 'The send executable does not exist. Run "init.py".' )
      sys.exit(1)

    if not os.path.isfile ( '../clients/receive' ) :
      print ( 'The receive executable does not exist. Run "init.py".' )
      sys.exit(1)


    # Make sure that the required directories actually exist.
    if False == os.path.isdir ( dispatch_install ) :
      print ( f"{dispatch_install} directory does not exist." )
      sys.exit(1)

    if False == os.path.isdir ( proton_root ) :
      print ( f"{proton_root} directory does not exist." )
      sys.exit(1)

    if False == os.path.isdir ( proton_install ) :
      print ( f"{proton_install} directory does not exist." )
      sys.exit(1)

    context["dispatch_install"] = dispatch_install
    context["proton_root"]      = proton_root
    context["proton_install"]   = proton_install



def make_test_dir ( ) :
    tests_root = "../test"
    if False == os.path.isdir ( tests_root ) :
      os.mkdir ( tests_root )
    now = datetime.datetime.now()
    time_string = now.strftime('%Y_%m_%d_%H_%M_%S')
    test_dir = tests_root + "/" + time_string
    if os.path.isdir ( test_dir ) :
      print ( f"Results dir {test_dir} already exists." )
      sys.exit(1)
    os.mkdir ( test_dir )
    os.mkdir ( test_dir + "/config" )
    context['test_dir'] = test_dir
    print ( f"Test dir is |{test_dir}|" )
   

    
def make_router ( command ) :
    router_name = command[0]
    threads = command[1]
    port = find_open_port()
    config_file_name = context['test_dir'] + "/config/" + router_name + ".conf"

    # Start a new dictionary for this router.
    context["routers"][router_name] = {}
    context["routers"][router_name]['threads'] = threads
    context["routers"][router_name]['port'] = port
    context["routers"][router_name]['config_file_name'] = config_file_name



def write_router_config ( router_name ) :
    threads = context["routers"][router_name]['threads']
    port    = context["routers"][router_name]['port']

    f = open ( context["routers"][router_name]['config_file_name'], "w" )
    f.write("router {\n")
    f.write( "    mode: interior\n")
    f.write(f"    id: {router_name}\n")
    f.write(f"    workerThreads: {threads}\n")
    f.write( "}\n")
    f.write( "listener {\n")
    f.write( "    role: normal\n")
    f.write( "    stripAnnotations: no\n")
    f.write( "    saslMechanisms: ANONYMOUS\n")
    f.write( "    host: 0.0.0.0\n")
    f.write( "    authenticatePeer: no\n")
    f.write(f"    port: {port}\n")
    f.write( "    linkCapacity: 250\n")
    f.write( "}\n")

    if 'inter_router_connector' in context['routers'][router_name] :
      port = context['routers'][router_name]['inter_router_connector']
      f.write( "connector {\n")
      f.write( "    role: inter-router\n")
      f.write( "    host: 0.0.0.0\n")
      f.write( "    saslMechanisms: ANONYMOUS\n")
      f.write(f"    port: {port}\n")
      f.write( "}\n")

    if 'inter_router_listener' in context['routers'][router_name] :
      port = context['routers'][router_name]['inter_router_listener']
      f.write( "listener {\n")
      f.write( "    role: inter-router\n")
      f.write( "    host: 0.0.0.0\n")
      f.write( "    saslMechanisms: ANONYMOUS\n")
      f.write(f"    port: {port}\n") 
      f.write( "}\n")

    f.close()





# All the executables use this environment.
def make_env ( ) :
    new_env = dict(os.environ)
    new_env["LD_LIBRARY_PATH"] = context['dispatch_install'] + \
                                 "/lib:"                     + \
                                 context['proton_install']   + \
                                 "/lib64"

    new_env["PYTHONPATH"] = context['dispatch_install']  + \
                            "/lib/qpid-dispatch/python:" + \
                            context['dispatch_install']  + \
                            "/lib/python3.9/site-packages"
    return new_env



def make_senders ( args ) :
    for i in range(args['n_senders']) :
        # Make a dictionary for this sender.
        context['sender_count'] += 1
        sender_name = 'send_' + str(context['sender_count'])
        context['clients_list'].append ( sender_name )
        context['senders'][sender_name] = {}
        print ( f'Made sender |{sender_name}|.' )

        # Store the original router name, even (especially) if
        # it is random. We will need to know that later, if we
        # are doing kill-and-replace.
        context['senders'][sender_name]['router'] = args['router']

        # The chosen_router is the router name that will be used
        # for the rest of this function. It will be the same as
        # the input arg router name, unless that name is 'random'.
        chosen_router = args['router']
        if chosen_router == 'random' :
          chosen_router = random.choice ( list(context['routers']) )

        context['senders'][sender_name]['router'] = chosen_router
        output_file_name = context['test_dir'] + "/" + sender_name + ".output"
        context['senders'][sender_name]['output_file_name'] = output_file_name
        context['senders'][sender_name]['n_messages'] = args['n_messages']

        # TODO -- distinguish between AMQP port and TCP, etc.
        port = str(context['routers'][chosen_router]['port'])
        context['senders'][sender_name]['port'] = port

        # Choose the sender's address randomly.
        addr = random.choice ( context['addresses'] )
        context['senders'][sender_name]['addr'] = addr



def make_receivers ( args ) :
    for i in range(args['n_receivers']) :
        # Make a dictionary for this receiver.
        context['receiver_count'] += 1
        receiver_name = 'recv_' + str(context['receiver_count'])
        context['clients_list'].append ( receiver_name )
        context['receivers'][receiver_name] = {}
        print ( f'Made receiver |{receiver_name}|.' )

        # Store the original router name, even (especially) if
        # it is random. We will need to know that later, if we
        # are doing kill-and-replace.
        context['receivers'][receiver_name]['router'] = args['router']

        # The chosen_router is the router name that will be used
        # for the rest of this function. It will be the same as
        # the input arg router name, unless that name is 'random'.
        chosen_router = args['router']
        if chosen_router == 'random' :
          chosen_router = random.choice ( list(context['routers']) )

        output_file_name = context['test_dir'] + "/" + receiver_name + ".output"
        context['receivers'][receiver_name]['output_file_name'] = output_file_name
        context['receivers'][receiver_name]['n_messages'] = args['n_messages']
        context['receivers'][receiver_name]['report'] = args['report_freq']

        port = str(context['routers'][chosen_router]['port'])
        context['receivers'][receiver_name]['port'] = port

        # Choose the sender's address randomly.
        addr = random.choice ( context['addresses'] )
        context['receivers'][receiver_name]['addr'] = addr



def start_router ( router_name ) :
    config_file_name = context['test_dir'] + "/config/" + router_name + ".conf"
    command = [ context['router'], '--config', config_file_name ]

    output_file_name = context['test_dir'] + "/" + router_name + ".output"

    output_file = open ( output_file_name, "w" ) 
    process = subprocess.Popen ( command, 
                                 env = make_env(),
                                 stderr = output_file )
    context['routers'][router_name]['process'] = process



def stop_router ( router_name ) :
    if 'process' in context['routers'][router_name] :
        router_process = context['routers'][router_name]['process']
        router_process.terminate()
        print ( f"Stopped router: |{router_name}|")
    else :
        print ( f"Not stopping router |{router_name}|, because it was not started.")



def stop_receiver ( recv ) :
    if 'process' in context['receivers'][recv] :
        proc = context['receivers'][recv]['process']
        proc.terminate()
        print ( f"Stopped receiver: |{recv}|")
    else :
        print ( f"Not stopping receiver |{recv}|, because it was not started.")



def stop_sender ( send ) :
    if 'process' in context['senders'][send] :
        proc = context['senders'][send]['process']
        proc.terminate()
        print ( f"Stopped sender: |{send}|")
    else :
        print ( f"Not stopping sender |{send}|, because it was not started.")



def start_routers ( ) :
    for router in context['routers'] :
      write_router_config ( router )
      start_router ( router )
      print ( f"Started router |{router}|." )



def start_receiver ( name ) :
    chosen_router = context['receivers'][name]['router']
    if chosen_router == "random" :
      chosen_router = random.choice ( list(context['routers']) )

    port   = str(context['routers'][chosen_router]['port'])
    out    = context['receivers'][name]['output_file_name']
    addr   = context['receivers'][name]['addr']
    n_msg  = context['receivers'][name]['n_messages']

    output_file = open ( out, "w" ) 
    command = [ '../clients/receive', \
                'port', port,         \
                'address', addr,      \
                'message_count', str(n_msg) ]

    proc = subprocess.Popen ( command, 
                              env = make_env(),
                              stdout = output_file )
    context['receivers'][name]['process'] = proc
    print ( f"Started receiver {name} as proc {proc.pid} on router {chosen_router} on addr {addr}." )



def start_receivers ( ) :
    for name in  context['receivers'] :
        start_receiver ( name )



def start_sender ( name ) :
    chosen_router = context['senders'][name]['router']
    if chosen_router == "random" :
      chosen_router = random.choice ( list(context['routers']) )

    port   = context['senders'][name]['port']
    out    = context['senders'][name]['output_file_name']
    addr   = context['senders'][name]['addr']
    n_msg  = context['senders'][name]['n_messages']

    output_file = open ( out, "w" ) 
    command = [ '../clients/send', \
                'port', port,      \
                'address', addr,   \
                'message_count', str(n_msg) ]

    proc = subprocess.Popen ( command, 
                              env = make_env(),
                              stdout = output_file )
    context['senders'][name]['process'] = proc
    print ( f"Started sender {name} as proc {proc.pid} on router {chosen_router} on addr {addr}." )



def start_senders ( ) :
    for name in context['senders'] :
        start_sender ( name )



def start ( ) :
    print ( "Starting!" )
    start_routers ( )
    delay = 5
    print ( f"Waiting {delay} seconds for routers." )
    time.sleep ( delay )
    print ( "Starting receivers." )
    start_receivers ( )
    time.sleep ( delay )
    print ( "Starting senders." )
    start_senders ( )



def stop ( ) :
    print ( "Stopping!" )
    routers_are_still_running ( )
    for recv in context['receivers'] :
      stop_receiver ( recv )
    for send in context['senders'] :
      stop_sender ( send )
    for router in context['routers'] :
      stop_router ( router )



def make_addresses ( n ) :
    print ( f"Making {n} addresses." )
    for i in range(int(n)) :
      context['addresses'].append ( "addr_" + str(i+1) )
    print ( f"There are now {len(context['addresses'])} addresses." )



# Set up connectivity between routers 1 and 2.
# This function is called before startup, and only
# stores information in the router data structures
# that is used later. During startup.
def connect ( router_1, router_2 ) :
    print ( f"Connect router |{router_1}| to router |{router_2}|" )
    port = find_open_port ( )
    context['routers'][router_1]['inter_router_connector'] = port
    context['routers'][router_2]['inter_router_listener']  = port



def read_args ( words, args ) :
    for i in range(len(words)-1) :
      if words[i] in args :
        args[words[i]] = words[i+1]



def read_commands ( file_name ) :
    with open(file_name) as f:
        content = f.readlines()
    for line in content :
      words = line.split()

      if words[0] == '#' :
        continue
      if words[0] == 'echo' :
        s = " "
        print(s.join(words[1:]))
      elif words[0] == 'router' :
        make_router ( words[1:] )
      elif words[0] == 'pause' :
        print ( f"pause for {words[1]} seconds." )
        time.sleep ( int(words[1]) )
      elif words[0] == 'start' :
        start ( )
      elif words[0] == 'stop' :
        stop ( )
      elif words[0] == 'receivers' :
        receiver_args = default_receiver_args.copy()
        read_args ( words[1:], receiver_args )
        make_receivers ( receiver_args )
      elif words[0] == 'senders' :
        sender_args = default_sender_args.copy()
        read_args ( words[1:], sender_args )
        make_senders ( sender_args )
      elif words[0] == 'addresses' :
        make_addresses ( words[1] )
      elif words[0] == 'kill_and_replace_clients' :
        kill_and_replace_clients ( words[1] )
      elif words[0] == 'connect' :
        connect ( words[1], words[2] )
      else :
        print ( f"Unknown command: |{words[0]}|" )



def routers_are_still_running ( ) :
    for name in context['routers'] :
        result = context['routers'][name]['process'].poll()
        if result != None :
            print ( f"error: router |{name}| is no longer running." )
            return False
    print ( "All routers are still running." )
    return True



def kill_and_replace_clients ( n ) :
    for i in range ( int(n) ) :
      name = random.choice ( context['clients_list'] )
      if name.startswith ( 'recv' ) :
          stop_receiver ( name )
          time.sleep ( 2 )
          start_receiver ( name )
          time.sleep ( 1 )
      elif name.startswith ( 'send' ) :
          stop_sender ( name )
          time.sleep ( 2 )
          start_sender ( name )
          time.sleep ( 1 )
      
      print ( f"{i+1} of {n} clients have been killed and replaced." )

      if not routers_are_still_running ( ) :
          sys.exit(1)

      print ( f"Killed and replaced |{name}|" )
      print ( f"Program has been running for {int(time.time() - context['start_time'])} seconds." )
      print (  "------------------------------------------\n" )
    


# Main program ====================================

context['start_time'] = time.time()

find_router()
if context['router'] == None:
  print ( "No qdrouterd in path." )
  sys.exit(1)

check_env ( )

print ( f"using dispatch_install: {context['dispatch_install']}" )
print ( f"using proton_root:      {context['proton_root']}" )
print ( f"using proton_install:   {context['proton_install']}" )
print ( f"using qdrouterd:        {context['router']}" )

make_test_dir ( )

read_commands ( sys.argv[1] )


