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
package org.apache.solr.update;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class ParallelizedPeerSyncTest extends BaseDistributedSearchTestCase {
  private static int ulogNumRecordsToKeep = 100000;
  private static int numVersions = ulogNumRecordsToKeep;  // number of versions to use when syncing
  private static int peerSyncParallelismThreshold = 100;
  
  // we cache 100 DBQs to handle reordered updates
  private static int maxCachedDBQs = 100;
  
  private final String FROM_LEADER = DistribPhase.FROMLEADER.toString();

  private ModifiableSolrParams seenLeader = 
    params(DISTRIB_UPDATE_PARAM, FROM_LEADER);
  
  public ParallelizedPeerSyncTest() {
    stress = 0;

    // TODO: a better way to do this?
    configString = "solrconfig-tlog.xml";
    schemaString = "schema.xml";
  }
  
  @Override
  public void distribSetUp() throws Exception {
    // tlog gets deleted after node restarts if we use CachingDirectoryFactory.
    // make sure that tlog stays intact after we restart a node
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", String.valueOf(ulogNumRecordsToKeep));
    System.setProperty("solr.peerSync.parallelismThreshold", String.valueOf(peerSyncParallelismThreshold));
    System.setProperty("solr.ulog.maxNumLogsToKeep", String.valueOf(100));
    
    super.distribSetUp();
  }
  
  @Override
  public void distribTearDown() throws Exception {
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.ulog.numRecordsToKeep");
    System.clearProperty("solr.peerSync.parallelismThreshold");
    System.clearProperty("solr.ulog.maxNumLogsToKeep");
    super.distribTearDown();
  }
  
  
  @Test
  @ShardsFixed(num = 2)
  @Repeat(iterations=50)
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    SolrClient client0 = clients.get(0);
    SolrClient client1 = clients.get(1);

    
    //add a few docs to establish frame of reference
    long v = 0;
    List<SolrInputDocument> docsToAdd = new ArrayList<>();
    for(int i=0; i < 10000; i ++) {      
      v++;
      docsToAdd.add(sdoc("id",String.valueOf(v),"_version_",v));
    }
    add(client0, seenLeader, docsToAdd.toArray(new SolrInputDocument[0]));
    add(client1, seenLeader, docsToAdd.toArray(new SolrInputDocument[0]));
    client0.commit(); client1.commit();
    queryAndCompare(params("q", "*:*", "sort", "_version_ desc"), client0, client1);
    
    // add a document and ensure sync    
    v++;
    add(client0, seenLeader, sdoc("id",String.valueOf(v),"_version_",v));
    assertSync(client1, numVersions, true, shardsArr[0]);
    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*", "sort", "_version_ desc"), client0, client1);
    
    // add a few documents (more than peerSyncParallelismThreshold) and ensure sync
    docsToAdd = new ArrayList<>();
    for(int i=0; i < 20000; i++) {
        v++;
        docsToAdd.add(sdoc("id",String.valueOf(v),"_version_",v));
        if(i % 1000 == 0) {
          add(client0, seenLeader, docsToAdd.toArray(new SolrInputDocument[0]));
          client0.commit();
          docsToAdd = new ArrayList<>();
        }
    }
    // add remaining docs
    if(docsToAdd.size() > 0) {
      add(client0, seenLeader, docsToAdd.toArray(new SolrInputDocument[0]));
    }
    
    long start = System.nanoTime();
    assertSync(client1, numVersions, true, shardsArr[0]);
    System.out.println("############ Time For PeerSync:" + (System.nanoTime() - start));
    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*", "sort", "_version_ desc"), client0, client1);
  }

  
  @Test
  @ShardsFixed(num = 2)
  //@Repeat(iterations=5)
  public void testWithReorderedDBQs() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    SolrClient client0 = clients.get(0);
    SolrClient client1 = clients.get(1);

    
    //add a few docs to establish frame of reference
    long v = 0;
    List<SolrInputDocument> docsToAdd = new ArrayList<>();
    for(int i=0; i < 1000; i ++) {      
      v++;
      docsToAdd.add(sdoc("id",String.valueOf(v),"_version_",v));
    }
    add(client0, seenLeader, docsToAdd.toArray(new SolrInputDocument[0]));
    add(client1, seenLeader, docsToAdd.toArray(new SolrInputDocument[0]));
    client0.commit(); client1.commit();
    queryAndCompare(params("q", "*:*", "sort", "_version_ desc"), client0, client1);
    
    // add a document and ensure sync    
    v++;
    add(client0, seenLeader, sdoc("id",String.valueOf(v),"_version_",v));
    assertSync(client1, numVersions, true, shardsArr[0]);
    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*", "sort", "_version_ desc"), client0, client1);
    
    // add a few documents (more than peerSyncParallelismThreshold) and ensure sync
    /*docsToAdd = new ArrayList<>();
    for(int i=0; i < peerSyncParallelismThreshold + random().nextInt(20); i++) {
        v++;
        docsToAdd.add(sdoc("id",String.valueOf(v),"_version_",v));
        if(random().nextInt(40) % 5 == 0) {
          add(client0, seenLeader, docsToAdd.toArray(new SolrInputDocument[0]));
          docsToAdd = new ArrayList<>();
        }
    }
    // add remaining docs
    if(docsToAdd.size() > 0) {
      add(client0, seenLeader, docsToAdd.toArray(new SolrInputDocument[0]));
    }
    client0.commit();
    assertSync(client1, numVersions, true, shardsArr[0]);
    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*", "sort", "_version_ desc"), client0, client1);*/
    
    int numDBQs = 0;
    // add/delete a bunch of documents (at least as much as parallelization threshold documents to the leader
    // issue at least maxCachedDBQs deletes
    for(int i=0; (i < (peerSyncParallelismThreshold * 3)) && (numDBQs < maxCachedDBQs) ; i++) {
      v++; 
      // issue a delete once in a while
      if(v % 3 == 0) {
        long idToDel = random().nextInt((int) (v-1));
        delQ(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",Long.toString(-v)), "id:" + idToDel);
        numDBQs++;
      } else {
        add(client0, seenLeader, sdoc("id",String.valueOf(v),"_version_",v));
        v++;
        add(client0, seenLeader, sdoc("id",String.valueOf(v),"_version_",v));
      }
    }
    client0.commit(); 

    long start = System.nanoTime();
    assertSync(client1, numVersions, true, shardsArr[0]);
    System.out.println("############ Time For PeerSync:" + (System.nanoTime() - start));
    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*", "sort", "_version_ desc"), client0, client1);
  }


  void assertSync(SolrClient client, int numVersions, boolean expectedResult, String... syncWith) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions",Integer.toString(numVersions), "sync", StrUtils.join(Arrays.asList(syncWith), ',')));
    NamedList rsp = client.request(qr);
    assertEquals(expectedResult, (Boolean) rsp.get("sync"));
  }

}
