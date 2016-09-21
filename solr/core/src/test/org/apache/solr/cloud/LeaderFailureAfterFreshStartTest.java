/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.ZkTestServer.LimitViolationAction;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.Slice.State;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.handler.ReplicationHandler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Test for SOLR-9446
 *
 * This test is modeled after SyncSliceTest
 */
@Slow
public class LeaderFailureAfterFreshStartTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean success = false;
  int docId = 0;

  List<CloudJettyRunner> nodesDown = new ArrayList<>();

  @Override
  public void distribTearDown() throws Exception {
    if (!success) {
      printLayoutOnTearDown = true;
    }
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.ulog.numRecordsToKeep");
    System.clearProperty("tests.zk.violationReportAction");
    super.distribTearDown();
  }

  public LeaderFailureAfterFreshStartTest() {
    super();
    sliceCount = 1;
    fixShardCount(3);
  }

  protected String getCloudSolrConfig() {
    return "solrconfig-tlog.xml";
  }

  @Override
  public void distribSetUp() throws Exception {
    // tlog gets deleted after node restarts if we use CachingDirectoryFactory.
    // make sure that tlog stays intact after we restart a node
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    System.setProperty("tests.zk.violationReportAction", LimitViolationAction.IGNORE.toString());
    super.distribSetUp();
  }

  @Test
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);

    try {
      CloudJettyRunner initialLeaderJetty = shardToLeaderJetty.get("shard1");
      List<CloudJettyRunner> otherJetties = getOtherAvailableJetties(initialLeaderJetty);
      
      log.info("Leader node_name: {},  url: {}", initialLeaderJetty.coreNodeName, initialLeaderJetty.url);
      for (CloudJettyRunner cloudJettyRunner : otherJetties) {
        log.info("Nonleader node_name: {},  url: {}", cloudJettyRunner.coreNodeName, cloudJettyRunner.url);
      }
      
      CloudJettyRunner secondNode = otherJetties.get(0);
      CloudJettyRunner freshNode = otherJetties.get(1);
      
      // shutdown a node to simulate fresh start
      otherJetties.remove(freshNode);
      forceNodeFailures(Arrays.asList(freshNode));

      del("*:*");
      waitForThingsToLevelOut(30);

      checkShardConsistency(false, true);

      // index a few docs and commit
      for (int i = 0; i < 100; i++) {
        indexDoc(id, docId, i1, 50, tlong, 50, t1,
            "document number " + docId++);
      }
      commit();
      waitForThingsToLevelOut(30);

      checkShardConsistency(false, true);

      // start the freshNode 
      ChaosMonkey.start(freshNode.jetty);
      nodesDown.remove(freshNode);

      waitTillNodesActive();
      waitForThingsToLevelOut(30);
      
      //TODO check how to see if fresh node went into recovery (may be check count for replication handler on new leader) 
      
      long numRequestsBefore = (Long) secondNode.jetty
          .getCoreContainer()
          .getCores()
          .iterator()
          .next()
          .getRequestHandler(ReplicationHandler.PATH)
          .getStatistics().get("requests");
      
      // shutdown the original leader
      log.info("Now shutting down initial leader");
      forceNodeFailures(Arrays.asList(initialLeaderJetty));
      waitForNewLeader("shard1", (Replica)initialLeaderJetty.client.info  , 15);
      log.info("Updating mappings from zk");
      updateMappingsFromZk(jettys, clients, true);
      
      long numRequestsAfter = (Long) secondNode.jetty
          .getCoreContainer()
          .getCores()
          .iterator()
          .next()
          .getRequestHandler(ReplicationHandler.PATH)
          .getStatistics().get("requests");

      assertEquals("Node went into replication", numRequestsBefore, numRequestsAfter);
      
      success = true;
    } finally {
      System.clearProperty("solr.disableFingerprint");
    }
  }


  private void forceNodeFailures(List<CloudJettyRunner> replicasToShutDown) throws Exception {
    for (CloudJettyRunner replicaToShutDown : replicasToShutDown) {
      chaosMonkey.killJetty(replicaToShutDown);
      waitForNoShardInconsistency();
    }

    int totalDown = 0;

    Set<CloudJettyRunner> jetties = new HashSet<>();
    jetties.addAll(shardToJetty.get("shard1"));

    if (replicasToShutDown != null) {
      jetties.removeAll(replicasToShutDown);
      totalDown += replicasToShutDown.size();
    }

    jetties.removeAll(nodesDown);
    totalDown += nodesDown.size();

    assertEquals(getShardCount() - totalDown, jetties.size());

    nodesDown.addAll(replicasToShutDown);
  }

  
  private void waitForNewLeader(String shardName, Replica oldLeader, int maxWaitInSecs) throws Exception {
    log.info("Will wait for a node to become leader for {} secs", maxWaitInSecs);
    boolean waitForLeader = true;
    int i = 0;
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    zkStateReader.forceUpdateCollection(DEFAULT_COLLECTION);
    
    while(waitForLeader) {
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection coll = clusterState.getCollection("collection1");
      Slice slice = coll.getSlice(shardName);
      if(slice.getLeader() != oldLeader && slice.getState() == State.ACTIVE) {
        log.info("New leader got elected in {} secs", i);
        break;
      }
      
      if(i == maxWaitInSecs) {
        Diagnostics.logThreadDumps("Could not find new leader in specified timeout");
        zkStateReader.getZkClient().printLayoutToStdOut();
        fail("Could not find new leader even after waiting for " + maxWaitInSecs + "secs");
      }
      
      i++;
      Thread.sleep(1000);
    }
  }
    


  private void waitTillNodesActive() throws Exception {
    for (int i = 0; i < 60; i++) {
      Thread.sleep(3000);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection collection1 = clusterState.getCollection("collection1");
      Slice slice = collection1.getSlice("shard1");
      Collection<Replica> replicas = slice.getReplicas();
      boolean allActive = true;

      Collection<Replica> replicasToCheck = null;
      replicasToCheck = replicas.stream().filter(r -> nodesDown.contains(r.getName()))
          .collect(Collectors.toList());

      for (Replica replica : replicasToCheck) {
        if (!clusterState.liveNodesContain(replica.getNodeName()) || replica.getState() != Replica.State.ACTIVE) {
          allActive = false;
          break;
        }
      }
      if (allActive) {
        return;
      }
    }
    printLayout();
    fail("timeout waiting to see all nodes active");
  }

  
  private List<CloudJettyRunner> getOtherAvailableJetties(CloudJettyRunner leader) {
    List<CloudJettyRunner> candidates = new ArrayList<>();
    candidates.addAll(shardToJetty.get("shard1"));

    if (leader != null) {
      candidates.remove(leader);
    }

    candidates.removeAll(nodesDown);

    return candidates;
  }

  protected void indexDoc(Object... fields) throws IOException,
      SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();

    addFields(doc, fields);
    addFields(doc, "rnd_s", RandomStringUtils.random(random().nextInt(100) + 100));

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ModifiableSolrParams params = new ModifiableSolrParams();
    ureq.setParams(params);
    ureq.process(cloudClient);
  }

  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

}
