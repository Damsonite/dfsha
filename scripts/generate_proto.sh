#!/bin/bash

set -e

python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src/common \
    --grpc_python_out=./src/common \
    ./proto/storage.proto

# protoc emite un import absoluto ("import storage_pb2"), que falla al
# importar el modulo como src.common.storage_pb2_grpc porque src/common
# no esta en sys.path. Lo reescribimos al import del paquete real.
sed -i \
    's/^import storage_pb2 as storage__pb2$/from src.common import storage_pb2 as storage__pb2/' \
    ./src/common/storage_pb2_grpc.py

echo "Protocol Buffers generados correctamente."
