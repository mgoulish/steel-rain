#! /bin/bash

echo "r: Running Steel Rain"
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


./steel_rain   


