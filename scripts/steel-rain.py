#! /usr/bin/python

import os
import sys
import subprocess
import shutil
import time
import datetime
import socket



context = { "routers"   : {},
            "receivers" : {},
            "senders"   : {},
            "addresses" : [] }



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
    print ( f"Test dir is |{test_dir}|\n" )
   
    
def make_router ( command ) :
    router_name = command[0]
    threads = command[1]
    # TODO -- store and use this!
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

    print ( f"list of routers is now: |{context['routers']}|\n" )




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



def start_sender ( router_name, addr ) :
    sender_name = 'send'
    output_file_name = context['test_dir'] + "/" + sender_name + ".output"
    port = str(context['routers'][router_name]['port'])
    command = [ '../clients/send', 'port', port, 'address', addr ]
    output_file_name = context['test_dir'] + "/" + sender_name + ".output"
    output_file = open ( output_file_name, "w" ) 
    process = subprocess.Popen ( command, 
                                 env = make_env(),
                                 stdout = output_file )
    context['senders'][sender_name] = process


def start_receiver ( router_name, addr ) :
    receiver_name = 'recv'
    output_file_name = context['test_dir'] + "/" + receiver_name + ".output"
    port = str(context['routers'][router_name]['port'])
    command = [ '../clients/receive', 'port', port, 'address', addr ]
    output_file_name = context['test_dir'] + "/" + receiver_name + ".output"
    output_file = open ( output_file_name, "w" ) 
    process = subprocess.Popen ( command, 
                                 env = make_env(),
                                 stdout = output_file )
    context['receivers'][receiver_name] = process





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
        print ( f"stopping router: |{router_name}|\n")
        router_process = context['routers'][router_name]['process']
        router_process.terminate()
    else :
        print ( f"Not stopping router |{router_name}|, because it was not started.\n")



def start_routers ( ) :
    print ( "Starting!\n" )
    for router in context['routers'] :
      start_router ( router )
      print ( f"Started router {router}.\n" )



def stop ( ) :
    print ( "Stopping!\n" )
    for router in context['routers'] :
      stop_router ( router )


def make_addresses ( n ) :
    print ( f"Making {n} addresses.\n" )
    for i in range(int(n)) :
      context['addresses'].append ( "addr_" + str(i+1) )
    print ( f"There are now {len(context['addresses'])} addresses.\n" )

def read_commands ( file_name ) :
    with open(file_name) as f:
        content = f.readlines()
    for line in content :
      words = line.split()
      if words[0] == 'echo' :
        s = " "
        print(s.join(words[1:]))
      elif words[0] == 'router' :
        make_router ( words[1:] )
      elif words[0] == 'pause' :
        print ( f"pause for {words[1]} seconds.\n" )
        time.sleep ( int(words[1]) )
      elif words[0] == 'start_routers' :
        start_routers ( )
      elif words[0] == 'stop' :
        stop ( )
      elif words[0] == 'recv' :
        # Pass router name and addr.
        start_receiver ( words[1], words[2] ) 
      elif words[0] == 'send' :
        # Pass router name and addr.
        start_sender ( words[1], words[2] )
      elif words[0] == 'addresses' :
        make_addresses ( words[1] )
      else :
        print ( f"Unknown command: |{words[0]}|\n" )



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


