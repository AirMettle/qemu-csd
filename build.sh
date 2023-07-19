#!/bin/sh
#
# qemu build script
#
# Copyright (C) 2023 AirMettle, Inc.
#
# This code is licensed under the GNU GPL v2 or later.

DIR=`pwd`
mkdir -p $DIR/duckdb
pushd $DIR/duckdb
os_type=$(uname -s)

if [[ $os_type == *"Darwin"* ]]; then
  if [[ -f "libduckdb.dylib" ]]; then
      echo "Duckdb lib file already exists."
  else
    wget https://github.com/duckdb/duckdb/releases/download/v0.8.0/libduckdb-osx-universal.zip --no-check-certificate
    unzip libduckdb-osx-universal.zip
    rm libduckdb-osx-universal.zip
    rm duckdb.h*
  fi
elif [[ $os_type == *"Linux"* ]]; then
  if [[ -f "libduckdb.so" ]]; then
      echo "Duckdb lib file already exists."
  else
    wget https://github.com/duckdb/duckdb/releases/download/v0.8.0/libduckdb-linux-amd64.zip
    unzip libduckdb-linux-amd64.zip
    rm libduckdb-linux-amd64.zip
    rm duckdb.h*
  fi
elif [[ $os_type == *"CYGWIN"* || $os_type == *"MINGW"* ]]; then
  if [[ -f "duckdb.lib" && -f "duckdb.dll" ]]; then
      echo "Duckdb lib file already exists."
  else
      wget https://github.com/duckdb/duckdb/releases/download/v0.8.0/libduckdb-windows-amd64.zip
      unzip libduckdb-windows-amd64.zip
      rm libduckdb-windows-amd64.zip
      rm duckdb.h*
  fi
else
  echo "Unknown operating system."
  exit
fi

popd

mkdir -p build
cd build
../configure --target-list=x86_64-softmmu
make -j4
