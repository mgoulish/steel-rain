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
            "sender_count"   : 0,
            "receiver_count" : 0 }



def get_open_port():
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
    port = get_open_port()
    config_file_name = context['test_dir'] + "/config/" + router_name + ".conf"
    f = open ( config_file_name, "w" )
    f.write("router {\n")
    f.write( "    mode: interior\n")
    f.write(f"    id: {router_name}\n")
    f.write(f"    workerThreads: {threads}\n")
    f.write( "}\n")
    f.write( "listener {\n")
    f.write( "    stripAnnotations: no\n")
    f.write( "    saslMechanisms: ANONYMOUS\n")
    f.write( "    host: 0.0.0.0\n")
    f.write( "    role: normal\n")
    f.write( "    authenticatePeer: no\n")
    f.write(f"    port: {port}\n")
    f.write( "    linkCapacity: 250\n")
    f.write( "}\n")
    f.close()

    # Start a new dictionary for this router.
    context["routers"][router_name] = {}
    context["routers"][router_name]['port'] = port

    print ( f"list of routers is now: |{context['routers']}|" )




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



def make_senders ( router_name, n ) :
    for i in range(int(n)) :
        # Make a dictionary for this sender.
        context['sender_count'] += 1
        sender_name = 'send' + str(context['sender_count'])
        context['senders'][sender_name] = {}

        # Get the stuff we will need to start it.
        context['senders'][sender_name]['router'] = router_name
        output_file_name = context['test_dir'] + "/" + sender_name + ".output"
        context['senders'][sender_name]['output_file_name'] = output_file_name

        port = str(context['routers'][router_name]['port'])
        context['senders'][sender_name]['port'] = port

        # Choose the sender's address randomly.
        addr = random.choice ( context['addresses'] )
        context['senders'][sender_name]['addr'] = addr




def make_receivers ( router_name, n ) :
    for i in range(int(n)) :
        # Make a dictionary for this receiver.
        context['receiver_count'] += 1
        receiver_name = 'recv' + str(context['receiver_count'])
        context['receivers'][receiver_name] = {}

        # Get the stuff we will need to start it.
        context['receivers'][receiver_name]['router'] = router_name
        output_file_name = context['test_dir'] + "/" + receiver_name + ".output"
        context['receivers'][receiver_name]['output_file_name'] = output_file_name

        port = str(context['routers'][router_name]['port'])
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
      start_router ( router )
      print ( f"Started router |{router}|." )



def start_receivers ( ) :
    for name in  context['receivers'] :
        port   = context['receivers'][name]['port']
        out    = context['receivers'][name]['output_file_name']
        addr   = context['receivers'][name]['addr']
        output_file = open ( out, "w" ) 
        command = [ '../clients/receive', \
                    'port', port,         \
                    'address', addr ]
        proc = subprocess.Popen ( command, 
                                  env = make_env(),
                                  stdout = output_file )
        context['receivers'][name]['process'] = proc
        print ( f"Started receiver {name} {proc.pid}" )



def start_senders ( ) :
    for name in  context['senders'] :
        port   = context['senders'][name]['port']
        out    = context['senders'][name]['output_file_name']
        addr   = context['senders'][name]['addr']
        output_file = open ( out, "w" ) 
        command = [ '../clients/send', \
                    'port', port,      \
                    'address', addr ]
        proc = subprocess.Popen ( command, 
                                  env = make_env(),
                                  stdout = output_file )
        context['senders'][name]['process'] = proc
        print ( f"Started sender {name} {proc.pid}" )



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
        make_receivers ( words[1], words[2] ) 
        #print ( f"receivers: {context['receivers']}" )
      elif words[0] == 'senders' :
        make_senders ( words[1], words[2] )
        #print ( f"senders: {context['senders']}" )
      elif words[0] == 'addresses' :
        make_addresses ( words[1] )
      else :
        print ( f"Unknown command: |{words[0]}|" )



# Main program ====================================

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


