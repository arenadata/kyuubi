<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Helm Chart for Apache Kyuubi

[Apache Kyuubi](https://kyuubi.apache.org) is a distributed and multi-tenant gateway to provide serverless SQL on Data Warehouses and Lakehouses.

## Introduction

This chart runs a stateless Kyuubi server in a [Kubernetes](http://kubernetes.io) Deployment
with the [Helm](https://helm.sh) package manager, one release per user or team. The server stands
on its own:

- no ZooKeeper: `kyuubi.ha.client.class` is the `KubernetesDiscoveryClient` of
  `extensions/server/kyuubi-kubernetes-plugin`, which keeps what Kyuubi would put in ZooKeeper in
  the server's memory;
- no metadata store: REST and Spark Connect, the frontends that need one, are refused;
- no engines of its own: engines are run by operators behind Services, and the server only
  connects to them. A session whose engine is not running fails; nothing is started in its place.

### One release per user or team

A pod that goes away ends every session it holds, whatever the replica count, and one team's load
needs no sharing out. So each user or team gets its own release with one replica:

```shell
helm install kyuubi-analytics charts/kyuubi -n analytics \
  --set discovery.labelSelector=app.apache.org/team=analytics
```

Each release sees only its own team's Services - by namespace (`discovery.namespaces`) or by
label (`discovery.labelSelector`; a Service must still carry `app.apache.org/discovery` for its
type) - and the `kyuubi.apache.org/users` annotation narrows a profile further
to some of the team. Clients connect to the release's Service:

```
jdbc:hive2://kyuubi-analytics-thrift-binary.analytics.svc:10009/
```

From outside the cluster, make that Service a `LoadBalancer` or `NodePort`
(`server.thriftBinary.service.type`).

A rollout starts the new pod before stopping the old one: the old pod's sessions still end, but
their clients can reconnect at once. When the pod or its node fails, the server is gone until
Kubernetes starts another one.

## Requirements

- Kubernetes cluster
- Helm 3.0+
- An image with the `kyuubi-kubernetes-plugin` jar in `$KYUUBI_HOME/jars`; the stock
  `apache/kyuubi` image does not have it. `extensions/server/kyuubi-kubernetes-plugin/deploy/Dockerfile`
  builds one.

## Engines

The servers watch the Services labelled `discovery.labelSelector` (`app.apache.org/discovery`,
whose value is the engine type) in
`discovery.namespaces` (the release's own unless listed). A Service is an engine profile, declared
by its annotations, and a port named `kyuubi` - or the one its `kyuubi.apache.org/port-name`
annotation names - is where the profile's engine runs:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: spark4
  labels:
    app.apache.org/discovery: SPARK_SQL
  annotations:
    kyuubi.apache.org/default: "true"
spec:
  selector:
    app: spark-thrift-server
  ports:
    - name: kyuubi
      port: 10009
```

Changes to these Services take effect without a restart. The annotation vocabulary is in the
plugin's README.

The chart creates a Role and a RoleBinding in each watched namespace with `get`, `list` and
`watch` on Services, and nothing else: the servers write nothing to the Kubernetes API.

## Configuration

The chart renders `kyuubi-defaults.conf` itself: frontends, the discovery client, the session conf
advisor, the watched namespaces and the share level (`engine.shareLevel`, `SERVER` by default).
Anything else goes into `kyuubiConf.defaults`, which is appended to it. The file, the other
`kyuubiConf.files` and the `hadoopConf.files` are kept in Secrets, not ConfigMaps: they may carry
passwords and principals.

```yaml
kyuubiConf:
  defaults: |
    kyuubi.authentication=KERBEROS
    kyuubi.kinit.principal=kyuubi/_HOST@EXAMPLE.COM
    kyuubi.kinit.keytab=/opt/kyuubi/conf/kyuubi.keytab
  filesFrom:
    - secret:
        name: kyuubi-keytab
        items:
          - key: keytab
            path: kyuubi.keytab
```

Setting `kyuubi.frontend.protocols`, `kyuubi.ha.addresses`, `kyuubi.ha.client.class`,
`kyuubi.session.conf.advisor` or `kyuubi.plugin.kubernetes.namespaces` there, a
`kyuubi-defaults.conf` in `kyuubiConf.files`, or enabling a frontend other than `thriftBinary`,
`thriftHttp` and `mysql` fails the rendering: each would bring state back.

Probes are TCP checks on the first enabled frontend's port. `replicaCount` and
`podDisruptionBudget` are there for whoever runs more than one replica; then Thrift HTTP, whose
session spans many requests, relies on the `sessionAffinity: ClientIP` its Service sets.

## Template rendering

When you want to test the template rendering, but not actually install anything. [Debugging templates](https://helm.sh/docs/chart_template_guide/debugging/) provide a quick way of viewing the generated content without YAML parse errors blocking.

There are two ways to render templates. It will return the rendered template to you so you can see the output.

- Local rendering chart templates
```shell
helm template --debug ../kyuubi
```
- Server side rendering chart templates
```shell
helm install --dry-run --debug --generate-name ../kyuubi
```

## Documentation

Configuration guide documentation for Kyuubi lives [on the website](https://kyuubi.readthedocs.io/en/master/configuration/settings.html#kyuubi-configurations). (Not just for Helm Chart)

## Contributing

Want to help build Apache Kyuubi? Check out our [contributing documentation](https://kyuubi.readthedocs.io/en/master/community/CONTRIBUTING.html).
