#! /usr/bin/python

import os
import sys
import subprocess


proton_root = os.environ.get('STEEL_RAIN_PROTON_ROOT')
if proton_root == None:
  print ( "please set STEEL_RAIN_PROTON_ROOT env var.")
  sys.exit(1)

proton_install = os.environ.get('STEEL_RAIN_PROTON_INSTALL')
if proton_install == None:
  print ( "please set STEEL_RAIN_PROTON_INSTALL env var.")
  sys.exit(1)


print ( f"using proton_root:    {proton_root}" )
print ( f"using proton_install: {proton_install}" )


def build_client ( client ) :
    print ( "\n-------------------------------" )
    print ( f"  building client {client}" )
    print (   "-------------------------------" )
    compile_command = []
    compile_command.append ( '/usr/bin/cc' )
    compile_command.append ( "-I" + proton_root + "/c/include " )
    compile_command.append ( "-I" + proton_root + "/c/src" )
    compile_command.append ( "-I" + proton_root + "/build/c/include" )
    compile_command.append ( "-I" + proton_root + "/build/c/src" )
    compile_command.extend ( ["-fvisibility=hidden", "-O2", "-g", "-DNDEBUG"] )
    compile_command.extend ( ["-std=c99", "-MD", "-MT"] )
    compile_command.extend ( ["../clients/" + client + ".c.o", "-MF"] )
    compile_command.append ( "../clients/" + client + ".c.o.d" )
    compile_command.extend ( ["-o", "../clients/" + client + ".c.o"] )
    compile_command.extend ( ["-c", "../clients/" + client + ".c"] )

    subprocess.call(compile_command)




for client in ['send', 'direct'] :
    build_client ( client )



