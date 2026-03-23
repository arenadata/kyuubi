Kyuubi Spark Connect
==============

### Connect to Kyuubi using gRPC

set the following options in `/etc/kyuubi/conf/kyuubi-defaults.conf`:

```
kyuubi.frontend.protocols=THRIFT_BINARY,REST,SPARK_CONNECT
kyuubi.frontend.spark.connect.bind.port=10199
kyuubi.frontend.spark.connect.ssl.enabled=true
```

#### "NOSASL" authentication type

set in `/etc/kyuubi/conf/kyuubi-defaults.conf`
```
kyuubi.authentication=NOSASL
```
run command:

```
export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt

pyspark3 --remote 'sc://vdmitriev-adh-orion-hadoop-3.ru-central1.internal:10199/;use_ssl=true;x-user-name=vdmitrieva'

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

#### with enabled KERBEROS (SPNEGO)

set in config `/etc/kyuubi/conf/kyuubi-defaults.conf`

```
kyuubi.authentication=KERBEROS

kyuubi.spnego.keytab=/etc/security/keytabs/HTTP.service.keytab
kyuubi.spnego.principal=HTTP/vdmitriev-adh-orion-hadoop-3.ru-central1.internal@RU-CENTRAL1.INTERNAL
```

##### Requirements for client

```
sudo apt-get install gcc

sudo apt-get install python3.10-dev

sudo apt-get install libkrb5-dev

python3.10 -m venv spark-venv

source spark-venv/bin/activate

pip install gssapi
```

```
export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt
export PYTHONPATH="/opt/pyspark3-python/lib/python3.10/site-packages/:$PYTHONPATH"
export PYTHONPATH="/usr/lib/spark3/python/lib/py4j-0.10.9.7-src.zip:/usr/lib/spark3/python/lib/pyspark.zip:$PYTHONPATH"
```

create and run python script to connect to kyuubi:

```
$ cat spark_connect_client.py:

import base64
import gssapi
import time
from pyspark.sql.connect.client import ChannelBuilder
from pyspark.sql.connect.session import SparkSession

HOST = "vdmitriev-adh-orion-hadoop-3.ru-central1.internal"
PORT = 10199

class KerberosChannelBuilder(ChannelBuilder):
    def __init__(self, url):
        super().__init__(url)

    def _get_token(self):
        now = time.time()
        service_name = gssapi.Name(
            f"HTTP@{self.host}",
            name_type=gssapi.NameType.hostbased_service)
        ctx = gssapi.SecurityContext(name=service_name, usage='initiate')
        token_bytes = ctx.step()
        token = base64.b64encode(token_bytes).decode('ascii')

        return token

    def metadata(self):
        arr = list(super().metadata()) + [('authorization', f'Negotiate {self._get_token()}')]
        print(arr)
        return arr

spark = SparkSession(connection=KerberosChannelBuilder(f"sc://{HOST}:{PORT}/;use_ssl=true"))

spark.sql("SELECT current_user()").show()
spark.sql("SELECT * FROM vdmitriev.table1_orc").show()
spark.sql("SELECT * FROM vdmitriev.table2_orc").show()

spark.stop()

print("Finished!")
```

```
python3.10 spark_connect_client.py
...

```

#### with enabled LDAP authentication

set in `/etc/kyuubi/conf/kyuubi-defaults.conf`

```
kyuubi.authentication=LDAP
```

##### Requirements for client

```
export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt
export PYTHONPATH="/opt/pyspark3-python/lib/python3.10/site-packages/:$PYTHONPATH"
export PYTHONPATH="/usr/lib/spark3/python/lib/py4j-0.10.9.7-src.zip:/usr/lib/spark3/python/lib/pyspark.zip:$PYTHONPATH"
```

```
$ cat spark_connect_client_ldap.py

import base64
import time
from urllib.parse import quote

from pyspark.sql.connect.session import SparkSession

HOST = "vdmitriev-adh-orion-hadoop-3.ru-central1.internal"
PORT = 10199
USER = "vdmitriev2"
PASS = "vdmitriev2"

raw_token = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
token = quote(raw_token, safe="")  # encodes = as %3D
print("token:",  token)

spark = SparkSession(f"sc://{HOST}:{PORT}/;use_ssl=true;token={token}")

spark.sql("SELECT current_user()").show()
spark.sql("SELECT * FROM vdmitriev.table1_orc").show()

spark.stop()

print("Finished!")

```
