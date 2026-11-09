import grpc

from src.common import metadata_pb2, metadata_pb2_grpc


def main():

    channel = grpc.insecure_channel("localhost:50051")

    grpc.channel_ready_future(channel).result(timeout=10)

    stub = metadata_pb2_grpc.MetadataServiceStub(channel)

    owner_id = "user-001"

    # 1. Health
    health = stub.Health(metadata_pb2.HealthRequest())

    print("\n1. Metadata Service:")
    print(health.status)

    # 2. Crear directorio
    directory = stub.Mkdir(
        metadata_pb2.MkdirRequest(owner_id=owner_id, name="documentos")
    )

    print("\n2. Crear directorio:")
    print(directory)

    directory_id = directory.entry_id

    # 3. Listar directorio raíz
    listing = stub.ListDir(metadata_pb2.ListDirRequest(owner_id=owner_id))

    print("\n3. Listar directorio raíz:")

    for entry in listing.entries:
        print(f"{entry.name} - {entry.type} - {entry.entry_id}")

    # 4. Crear archivo de prueba
    file = stub.RegisterFile(
        metadata_pb2.RegisterFileRequest(
            owner_id=owner_id,
            parent_id=directory_id,
            name="informe.txt",
            size=1024,
            checksum="test_checksum",
        )
    )

    print("\n4. Registrar archivo:")
    print(file)

    # 5. Consultar archivo
    stat = stub.Stat(
        metadata_pb2.StatRequest(owner_id=owner_id, entry_id=file.entry_id)
    )

    print("\n5. Consultar archivo:")
    print(stat)

    # 6. Consultar bloques
    locations = stub.LocateBlocks(
        metadata_pb2.LocateBlocksRequest(owner_id=owner_id, file_id=file.entry_id)
    )

    print("\n6. Localizar bloques:")
    print(locations)

    channel.close()


if __name__ == "__main__":
    main()
