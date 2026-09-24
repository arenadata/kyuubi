/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.plugin.kubernetes;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The profiles declared on Services, kept current.
 *
 * <p>Informers rather than polling: a Service that appears, changes or goes is reflected as soon as
 * the api server says so, and between events nothing is asked of it. One informer per namespace,
 * because the namespaces are listed rather than "all of them" - which is what lets the server run
 * with a Role in each instead of a ClusterRole over the whole cluster.
 */
public final class KubernetesClusterCatalog implements ClusterCatalog, AutoCloseable {

  /**
   * @param namespaces where to look; empty means every namespace, which needs cluster-wide read
   *     access to Services
   * @param labelSelector which Services declare a profile
   * @param annotationPrefix what their annotation keys start with
   */
  public static final class Settings {
    private final List<String> namespaces;
    private final String labelSelector;
    private final String annotationPrefix;

    public Settings(List<String> namespaces, String labelSelector, String annotationPrefix) {
      this.namespaces = Collections.unmodifiableList(new ArrayList<>(namespaces));
      this.labelSelector = labelSelector;
      this.annotationPrefix = annotationPrefix;
    }

    public List<String> namespaces() {
      return namespaces;
    }

    public String labelSelector() {
      return labelSelector;
    }

    public String annotationPrefix() {
      return annotationPrefix;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterCatalog.class);
  private static final long RESYNC_MILLIS = 10 * 60 * 1000L;

  private final KubernetesClient client;
  private final Settings settings;
  private final List<SharedIndexInformer<Service>> informers = new CopyOnWriteArrayList<>();
  private final AtomicReference<List<ClusterProfile>> snapshot = new AtomicReference<>(List.of());
  private final Object rebuildLock = new Object();

  public KubernetesClusterCatalog(KubernetesClient client, Settings settings) {
    this.client = client;
    this.settings = settings;
  }

  /** Starts watching; returns once every informer has its first listing. */
  public void start() {
    ResourceEventHandler<Service> handler =
        new ResourceEventHandler<>() {
          @Override
          public void onAdd(Service service) {
            rebuild();
          }

          @Override
          public void onUpdate(Service before, Service after) {
            rebuild();
          }

          @Override
          public void onDelete(Service service, boolean finalStateUnknown) {
            rebuild();
          }
        };
    if (settings.namespaces().isEmpty()) {
      informers.add(
          client
              .services()
              .inAnyNamespace()
              .withLabelSelector(settings.labelSelector())
              .inform(handler, RESYNC_MILLIS));
    } else {
      for (String namespace : settings.namespaces()) {
        informers.add(
            client
                .services()
                .inNamespace(namespace)
                .withLabelSelector(settings.labelSelector())
                .inform(handler, RESYNC_MILLIS));
      }
    }
    rebuild();
  }

  @Override
  public void close() {
    informers.forEach(SharedIndexInformer::stop);
    informers.clear();
  }

  @Override
  public List<ClusterProfile> profiles() {
    return snapshot.get();
  }

  /** Whether every informer is still watching. */
  public boolean isWatching() {
    return !informers.isEmpty() && informers.stream().allMatch(SharedIndexInformer::isWatching);
  }

  private void rebuild() {
    synchronized (rebuildLock) {
      List<ClusterProfile> profiles = new ArrayList<>();
      for (SharedIndexInformer<Service> informer : informers) {
        for (Service service : informer.getStore().list()) {
          // Per Service, not around the whole batch: one Service with a bad annotation must cost
          // that one profile, not every profile behind it.
          try {
            profiles.add(ServiceProfiles.toProfile(service, settings.annotationPrefix()));
          } catch (RuntimeException e) {
            LOG.warn(
                "Ignoring Service {}/{}, its annotations do not make an engine profile: {}",
                service.getMetadata().getNamespace(),
                service.getMetadata().getName(),
                e.getMessage());
          }
        }
      }
      profiles.sort(
          Comparator.comparing(ClusterProfile::name).thenComparing(ClusterProfile::source));
      List<ClusterProfile> next = List.copyOf(profiles);
      if (!next.equals(snapshot.getAndSet(next))) {
        LOG.info(
            "Engine profiles from Services changed, now {}: {}",
            next.size(),
            next.stream().map(ClusterProfile::toString).collect(Collectors.toList()));
      }
    }
  }
}
