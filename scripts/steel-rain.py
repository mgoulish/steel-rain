#! /usr/bin/python

import os
import sys
import subprocess
import shutil
import time
import datetime
import socket



context = {}



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
   
    
def make_router ( command ) :
    name = command[0]
    threads = command[1]
    port = get_open_port()
    config_file_name = context['test_dir'] + "/config/" + name + ".conf"
    print ( f"config file: {config_file_name}" )
    f = open ( config_file_name, "w" )
    f.write("router {\n")
    f.write( "    mode: interior\n")
    f.write(f"    id: {name}\n")
    f.write(f"    workerThreads: {threads}\n")
    f.write("}\n")
    f.write("listener {\n")
    f.write("    stripAnnotations: no\n")
    f.write("    saslMechanisms: ANONYMOUS\n")
    f.write("    host: 0.0.0.0\n")
    f.write("    role: normal\n")
    f.write("    authenticatePeer: no\n")
    f.write("    port: 5672\n")
    f.write("    linkCapacity: 250\n")
    f.write("}\n")
    f.close()

    router_env = dict(os.environ)
    router_env["LD_LIBRARY_PATH"] = context['dispatch_install'] + "/lib:" + context['proton_install'] + "/lib64"
    router_env["PYTHONPATH"] = context['dispatch_install'] + "/lib/qpid-dispatch/python:" + context['dispatch_install'] + "/lib/python3.9/site-packages"

    print ( f"LD_LIBRARY_PATH == |{router_env['LD_LIBRARY_PATH']}|\n" )
    print ( f"PYTHONPATH == |{router_env['PYTHONPATH']}|\n" )



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


# Main program ----------------------------

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

read_commands ( "./commands" )


