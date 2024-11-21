#! /bin/bash

echo "r: Steel Rain"
sleep 2

if test -f "./steel_rain"; then
  echo "r: ./steel_rain exists"
else
  echo "r: building ./steel_rain"
  go build ./steel_rain.go
  if test -f "./steel_rain"; then
    echo "r: ./steel_rain built"
  else
    echo "r: ./steel_rain build failed"
    exit
  fi
fi

rm -rf ./router_output
mkdir  ./router_output

rm -rf ./server_output
mkdir  ./server_output

rm -rf ./client_output
mkdir  ./client_output

sleep 3
echo "r: running..."
sleep 1

./steel_rain   


