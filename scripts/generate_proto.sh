#!/bin/bash

set -e

python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src/common \
    --grpc_python_out=./src/common \
    ./proto/*.proto

# protoc emite imports absolutos ("import storage_pb2"), que fallan al
# importar los modulos como src.common.*_pb2_grpc porque src/common no
# esta en sys.path. Los reescribimos al import del paquete real.
for generated in ./src/common/*_pb2_grpc.py; do
    sed -i -E \
        's/^import ([A-Za-z0-9_]+_pb2) as ([A-Za-z0-9_]+)$/from src.common import \1 as \2/' \
        "$generated"
done

echo "Protocol Buffers generados correctamente."
