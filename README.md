# DFSha

DFSha es un sistema de archivos distribuido sobre gRPC. El proyecto separa los
metadatos (namespace, usuarios y réplicas) del plano de datos (subida y descarga
de bloques), y replica cada bloque en tres Storage Nodes.

## Arquitectura

| Componente        | Puerto | Responsabilidad                                                                                                                |
| ----------------- | -----: | ------------------------------------------------------------------------------------------------------------------------------ |
| Metadata Service  |  50051 | Árbol de directorios, permisos, checksums, leases de subida y ubicación de réplicas. Persiste en PostgreSQL.                   |
| Data Service      |  50052 | Recibe y ensambla los bloques, aplica el anillo de hashing consistente, replica y repara.                                      |
| Storage Nodes (3) |  50061 | Almacenan los bloques en disco. Los puertos 50062 y 50063 del host también apuntan al puerto interno 50061 de los nodos 2 y 3. |
| Cliente / CLI     |      — | Se ejecuta en el host y habla con los servicios en `localhost:50051` y `localhost:50052`.                                      |
| PostgreSQL        |   5432 | Base de datos del Metadata Service.                                                                                            |

Los archivos se dividen en bloques de 4 MiB. Cada bloque se escribe con factor
de replicación 3 y quórum de escritura 2. El Data Service comprueba
periódicamente la salud de los nodos y repara réplicas dañadas o ausentes.

## Requisitos

- Docker Engine y Docker Compose v2.
- Python 3.14 para ejecutar el cliente, los tests y el benchmark en el host
  (es la misma versión que usa la imagen Docker).
- Puertos libres: `50051`, `50052`, `50061`, `50062`, `50063` y `5432`
  (o cambia `POSTGRES_PORT`).

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/Damsonite/dfsha.git
cd dfsha
```

### 2. Crear la configuración

```bash
cp .env.example .env
```

Edita `.env` y cambia al menos `POSTGRES_PASSWORD` y los tokens de desarrollo
antes de usar el sistema fuera de tu máquina.

### 3. Levantar la pila

```bash
docker compose up --build -d
docker compose ps
```

Al arrancar, el Metadata Service crea las tablas, los usuarios `user-001`,
`user-002` y `service`, y registra los tokens definidos en `.env`. Los Storage
Nodes se registran como `storage-1`, `storage-2` y `storage-3`.

Para seguir el arranque:

```bash
docker compose logs -f metadata
docker compose logs -f data
```

### 4. Preparar el cliente local

El cliente, los tests y el benchmark se ejecutan en el host y necesitan las
dependencias de `requirements.txt`:

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

Si no usas `uv`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Los módulos `src/common/*_pb2.py` ya están generados en el repositorio. Solo
necesitas regenerarlos si modificas los archivos de `proto/`:

```bash
./scripts/generate_proto.sh
```

## Configuración

Docker Compose lee automáticamente el archivo `.env` de la raíz. La siguiente
tabla resume las variables disponibles.

### PostgreSQL

| Variable            | Valor por defecto | Descripción                                                   |
| ------------------- | ----------------- | ------------------------------------------------------------- |
| `POSTGRES_DB`       | `dfsha`           | Nombre de la base de datos.                                   |
| `POSTGRES_USER`     | `dfsha`           | Usuario de PostgreSQL.                                        |
| `POSTGRES_PASSWORD` | —                 | Contraseña de PostgreSQL. Es obligatoria; cámbiala en `.env`. |
| `POSTGRES_PORT`     | `5432`            | Puerto publicado de PostgreSQL en el host.                    |

### Data Service

| Variable                        | Valor por defecto | Descripción                                                                                             |
| ------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------- |
| `MAX_UPLOAD_BYTES`              | `2147483648`      | Tamaño máximo de un archivo subido (2 GiB).                                                             |
| `UPLOAD_LEASE_TTL_SECONDS`      | `300`             | Vigencia inicial del lease de subida. Debe ser mayor que cero y no superar 3600 en el Metadata Service. |
| `HEALTH_CHECK_INTERVAL_SECONDS` | `5`               | Intervalo entre comprobaciones de salud de los Storage Nodes.                                           |
| `REPAIR_INTERVAL_SECONDS`       | `30`              | Intervalo entre ciclos de reparación de réplicas.                                                       |

### Autenticación

| Variable                  | Valor por defecto   | Descripción                                                                                      |
| ------------------------- | ------------------- | ------------------------------------------------------------------------------------------------ |
| `DFSHA_DEMO_TOKEN`        | `dev-demo-token`    | Token del usuario `user-001`.                                                                    |
| `DFSHA_SECOND_USER_TOKEN` | `dev-second-token`  | Token del usuario `user-002`.                                                                    |
| `DFSHA_SERVICE_TOKEN`     | `dev-service-token` | Token interno del usuario `service`, usado por el Data Service para operaciones administrativas. |
| `DFSHA_STORAGE_TOKEN`     | `dev-storage-token` | Token compartido entre el Data Service y los Storage Nodes.                                      |
| `DFSHA_TOKEN`             | `dev-demo-token`    | Token que usa el cliente. Debe corresponder al usuario indicado con `--user`.                    |

El Metadata Service guarda los tokens hasheados y los vuelve a sembrar cada vez
que arranca, por lo que un cambio en `.env` se aplica al reiniciar el servicio
`metadata`. El usuario `--user` y el token deben coincidir: `user-001` usa el
token demo, `user-002` usa el segundo token y solo el token de servicio puede
invocar las operaciones administrativas.

> Los tokens incluidos son de desarrollo y las conexiones gRPC no usan TLS.
> Cambia los tokens y añade una capa de transporte segura antes de desplegar en
> producción.

### Cliente

| Variable            | Valor por defecto | Descripción                    |
| ------------------- | ----------------- | ------------------------------ |
| `DATA_ENDPOINT`     | `localhost:50052` | Endpoint del Data Service.     |
| `METADATA_ENDPOINT` | `localhost:50051` | Endpoint del Metadata Service. |

### Storage Nodes

| Variable       | Valor por defecto | Descripción                                                                    |
| -------------- | ----------------- | ------------------------------------------------------------------------------ |
| `NODE_ID`      | `storage-unknown` | Identificador del nodo. Compose asigna `storage-1`, `storage-2` y `storage-3`. |
| `GRPC_PORT`    | `50061`           | Puerto gRPC del nodo dentro del contenedor.                                    |
| `STORAGE_PATH` | `/data`           | Directorio donde se guardan los bloques.                                       |

### Metadata Service

| Variable       | Valor por defecto                                                    | Descripción                                                                                   |
| -------------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `DATABASE_URL` | `postgresql+psycopg://dfsha:dfsha_dev_password@localhost:5432/dfsha` | Cadena de conexión a PostgreSQL. Compose la construye a partir de las variables `POSTGRES_*`. |

## Ejecución

### CLI

Con la pila levantada y el entorno virtual activo:

```bash
python -m src.client.cli <comando> [opciones]
```

Comandos disponibles:

| Comando                       | Descripción                                                                                         |
| ----------------------------- | --------------------------------------------------------------------------------------------------- |
| `put <archivo> [ruta_remota]` | Sube un archivo. Si se indica una ruta remota, resuelve el directorio padre; si no, usa `--parent`. |
| `get <ruta_o_id> <destino>`   | Descarga un archivo por ruta o por UUID.                                                            |
| `mkdir <ruta>`                | Crea un directorio. Usa `-p` para crear los padres y `--parent` para indicar el UUID del padre.     |
| `ls [ruta]`                   | Lista un directorio. Sin ruta lista la raíz.                                                        |
| `stat <ruta_o_id>`            | Muestra los metadatos de una entrada.                                                               |
| `rm <ruta_o_id>`              | Elimina un archivo o un directorio vacío.                                                           |

Opciones comunes:

- `--user`: usuario propietario de la operación. Por defecto `user-001`.
- `--parent`: UUID del directorio padre (alternativa a las rutas absolutas).
- `-p`, `--parents`: crea los directorios intermedios que falten.

Ejemplo completo:

```bash
# Crear un directorio y sus padres
python -m src.client.cli mkdir /documentos/facturas -p

# Subir un archivo a una ruta concreta
python -m src.client.cli put samples/test-large.bin /documentos/facturas/test-large.bin

# Listar y consultar metadatos
python -m src.client.cli ls /documentos/facturas
python -m src.client.cli stat /documentos/facturas/test-large.bin

# Descargar y verificar el checksum
python -m src.client.cli get /documentos/facturas/test-large.bin ./descargas/test-large.bin
sha256sum ./descargas/test-large.bin

# Eliminar
python -m src.client.cli rm /documentos/facturas/test-large.bin
python -m src.client.cli rm /documentos/facturas
```

Puedes cambiar el endpoint para un comando concreto:

```bash
DATA_ENDPOINT=192.168.1.20:50052 \
METADATA_ENDPOINT=192.168.1.20:50051 \
python -m src.client.cli ls /
```

El token y el usuario deben corresponder. Para operar como `user-002`:

```bash
DFSHA_TOKEN=dev-second-token python -m src.client.cli ls / --user user-002
```

### Tests

Los tests son de integración y necesitan la pila de Docker Compose en marcha.
Si los servicios no son accesibles en `localhost:50051` y `localhost:50052`, la
suite se marca como omitida.

```bash
pytest
pytest -m integration
```

También puedes ejecutar los escenarios por separado:

```bash
python -m scripts.test_auth
python -m scripts.test_upload_validation
python -m scripts.test_download_validation
python -m scripts.test_namespace_operations
python -m scripts.test_path_operations
python -m scripts.test_upload_leases
python -m scripts.test_storage
python -m scripts.test_storage_recovery
```

### Benchmark

El benchmark sube y descarga archivos de varios tamaños, mide latencias de
metadatos y escribe un informe JSON:

```bash
python -m scripts.benchmark \
  --sizes 1,4,8,16 \
  --runs 3 \
  --metadata-runs 50 \
  --output reports/benchmark.json
```

Opciones:

- `--sizes`: tamaños en MiB separados por comas (por defecto `1,4,8,16`).
- `--runs`: repeticiones de subida/descarga por tamaño (por defecto `3`).
- `--metadata-runs`: muestras por operación de metadatos (por defecto `30`).
- `--output`: ruta del informe JSON (por defecto `reports/benchmark.json`).

## Operación

```bash
# Detener los contenedores conservando los datos
docker compose stop

# Detener y eliminar los contenedores, conservando los volúmenes
docker compose down

# Ver el estado y los logs
docker compose ps
docker compose logs -f data
```

Los datos persisten en los volúmenes `postgres_data`, `storage_1_data`,
`storage_2_data` y `storage_3_data`. `docker compose down -v` elimina también
esos volúmenes y, con ellos, todos los archivos y metadatos.

## Estructura del proyecto

```text
proto/                  Definiciones gRPC
src/common/             Autenticación, hashing consistente, utilidades y stubs generados
src/metadata/           Metadata Service, modelos SQLAlchemy y repositorio
src/data/               Data Service: subida, descarga, replicación y reparación
src/storage/            Storage Nodes y gestión de bloques en disco
src/client/             Cliente gRPC y CLI
scripts/                Escenarios de prueba, benchmark y generación de protobuf
tests/                  Suite de integración de pytest
samples/                Archivos de ejemplo
reports/                Informes generados por el benchmark
```
