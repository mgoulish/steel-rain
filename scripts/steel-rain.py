#! /usr/bin/python

import os
import sys
import subprocess
import shutil
import time
import datetime
import socket



def get_open_port():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("",0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        return port


def find_router ( ) :
    return shutil.which ( 'qdrouterd' )


def check_env ( ):
    proton_root    = os.environ.get('STEEL_RAIN_PROTON_ROOT')
    if proton_root == None:
      print ( "please set STEEL_RAIN_PROTON_ROOT env var.")
      sys.exit(1)

    proton_install = os.environ.get('STEEL_RAIN_PROTON_INSTALL')
    if proton_install == None:
      print ( "please set STEEL_RAIN_PROTON_INSTALL env var.")
      sys.exit(1)


    if False == os.path.isdir ( proton_root ) :
      print ( f"{proton_root} directory does not exist." )
      sys.exit(1)

    if False == os.path.isdir ( proton_install ) :
      print ( f"{proton_install} directory does not exist." )
      sys.exit(1)

    return proton_root, proton_install



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
    return test_dir
   
    
def make_router ( command, test_dir ) :
    name = command[0]
    threads = command[1]
    port = get_open_port()
    config_file_name = test_dir + "/config/" + name + ".conf"
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



def read_commands ( file_name, test_dir ) :
    with open(file_name) as f:
        content = f.readlines()
    for line in content :
      words = line.split()
      if words[0] == 'echo' :
        s = " "
        print(s.join(words[1:]))
      elif words[0] == 'router' :
        make_router ( words[1:], test_dir )


# Main program ----------------------------

router = find_router()
if router == None:
  print ( "No qdrouterd in path." )
  sys.exit(1)

proton_root, proton_install = check_env ( )

print ( f"using proton_root:    {proton_root}" )
print ( f"using proton_install: {proton_install}" )
print ( f"using qdrouterd:      {router}" )

test_dir = make_test_dir()

read_commands ( "./commands", test_dir )
