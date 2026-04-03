# Kyuubi Spark Connect

## Connect to Kyuubi using gRPC

Add `kyuubi.frontend.protocols=SPARK_CONNECT` and set other options in Kyuubi configuration `/etc/kyuubi/conf/kyuubi-defaults.conf`:

```
kyuubi.frontend.protocols=THRIFT_BINARY,REST,SPARK_CONNECT
kyuubi.frontend.spark.connect.bind.port=10199
kyuubi.frontend.spark.connect.ssl.enabled=true
```

### Common requirements for python client

define `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` env variable (it points to path with ssl certificates):
```
export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt
```

**For KERBEROS and LDAP authentication types:**

Generate spark connect classes (TODO: think how to automate):
```
python3 -m grpc_tools.protoc --python_out=. --grpc_python_out=.  --proto_path=.  spark_connect_auth.proto
```
then define `PYTHONPATH` if you run python code non-interactively:

```
export PYTHONPATH="/opt/pyspark3-python/lib/python3.10/site-packages/:/usr/lib/spark3/python/lib/py4j-0.10.9.7-src.zip:/usr/lib/spark3/python/"

```

Deploy the spark code from PR (we'll use `KyuubiChannelBuilder` python class):
https://github.com/arenadata/spark/pull/27/changes

### There are several authentication types

#### "NOSASL" or "NONE" authentication type

Set in `/etc/kyuubi/conf/kyuubi-defaults.conf`

```
kyuubi.authentication=NOSASL
```

Run interactive pyspark shell command:

```
pyspark3 --remote 'sc://vdmitriev-adh-orion-hadoop-3.ru-central1.internal:10199/;use_ssl=true;x-user-name=vdmitriev'

Using Python version 3.10.4 (main, Apr 21 2025 10:41:58)
Client connected to the Spark Connect server at vdmitriev-adh-orion-hadoop-3.ru-central1.internal:10199
SparkSession available as 'spark'.
>>> spark.sql("select * from vdmitriev.table1_orc").show()
+-----+------------+
|   id|        data|
+-----+------------+
|  556|test data556|
...

```

#### KERBEROS authentication (SPNEGO)

set in config `/etc/kyuubi/conf/kyuubi-defaults.conf`

```
kyuubi.authentication=KERBEROS

kyuubi.spnego.keytab=/etc/security/keytabs/HTTP.service.keytab
kyuubi.spnego.principal=HTTP/vdmitriev-adh-orion-hadoop-3.ru-central1.internal@RU-CENTRAL1.INTERNAL
```

##### Requirements

Install the following packages (for ubuntu):
```
gcc, python3.10-dev, libkrb5-dev
```
and install python library:
```
gssapi
```

obtain Kerberos ticket-granting ticket:
```
kinit vdmitriev
```

you can run interactive pyspark shell command, define `KYUUBI_AUTH=kerberos` env variable:
```
KYUUBI_AUTH=kerberos pyspark3 --remote "sc://vdmitriev-adh-orion-hadoop-3.ru-central1.internal:10199/;use_ssl=true"
...
Using Python version 3.10.4 (main, Apr 21 2025 10:41:58)
Client connected to the Spark Connect server at vdmitriev-adh-orion-hadoop-3.ru-central1.internal:10199
SparkSession available as 'spark'.
>>> sql("select current_user()").show()
+--------------+
|current_user()|
+--------------+
|     vdmitriev|
+--------------+
```

or pass `auth="kerberos"` parameter to `KyuubiChannelBuilder` class in python code:

```
from pyspark.kyuubi.kyuubi_spark_connect import KyuubiChannelBuilder
from pyspark.sql.connect.session import SparkSession

HOST = "vdmitriev-adh-orion-hadoop-3.ru-central1.internal"
PORT = 10199

builder = KyuubiChannelBuilder(f"sc://{HOST}:{PORT}/;use_ssl=true", auth="kerberos")

spark = SparkSession(connection=builder)

spark.sql("SELECT current_user()").show()

spark.stop()

```

run this code:
```
$ python3 spark_connect_client_kerberos.py
+--------------+
|current_user()|
+--------------+
|     vdmitriev|
+--------------+
```

#### LDAP authentication

set in `/etc/kyuubi/conf/kyuubi-defaults.conf`

```
kyuubi.authentication=LDAP
```
see more about ldap parameters: https://kyuubi.readthedocs.io/en/master/security/ldap.html


##### Interactive mode (pyspark)

you can run interactive pyspark shell command, define `KYUUBI_AUTH=ldap`, `KYUUBI_USERNAME` and `KYUUBI_PASSWORD` env variables:
```
export KYUUBI_PASSWORD=vdmitriev2pass

KYUUBI_AUTH=ldap KYUUBI_USERNAME=vdmitriev2 pyspark3 --remote "sc://vdmitriev-adh-orion-hadoop-3.ru-central1.internal:10199/;use_ssl=true"
...
```

##### Python code

pass `auth="ldap"`, `username` and `password` parameters to `KyuubiChannelBuilder` class in python code:
```
builder = KyuubiChannelBuilder(f"sc://{HOST}:{PORT}/;use_ssl=true", auth="ldap", 
    username="vdmitriev2", password="vdmitriev2pass")
spark = SparkSession(connection=builder)

spark.sql("SELECT current_user()").show()
```

## Build kyuubi
```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
./build/dist --tgz --spark-provided --flink-provided --hive-provided --web-ui -Pjdbc-shaded -Pjava-8  -Pscala-2.13 -Pspark-3.5 -Pzookeeper-3.6 -Drat.skip=true
```

run spark-connect tests:
```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/vdmitriev/git/spark-3.5.4-bin-hadoop3-scala2.13
build/mvn test -pl kyuubi-server -Pjdbc-shaded -Pjava-8  -Pscala-2.13 -Pspark-3.5 -Pzookeeper-3.6  -Dsuites="org.apache.kyuubi.server.grpc.*,*SparkConnect*"
```
