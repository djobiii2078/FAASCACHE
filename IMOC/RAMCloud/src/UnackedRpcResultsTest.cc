/* Copyright (c) 2014-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "UnackedRpcResults.h"
#include "MasterService.h"
#include "MockCluster.h"

namespace RAMCloud {

using WireFormat::ClientLease;

/**
 * Fake log reference freer for testing. This freer does nothing on free
 * request.
 */
class DummyReferenceFreer : public AbstractLog::ReferenceFreer {
    virtual void freeLogEntry(Log::Reference ref) {
        TEST_LOG("freed <%" PRIu64 ">", ref.toInteger());
    }
};

class UnackedRpcResultsTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    ServerList serverList;
    MockCluster cluster;
    ServerConfig backup1Config;
    ServerId backup1Id;

    ServerConfig masterConfig;
    MasterService* service;
    Server* masterServer;

    DummyReferenceFreer freer;
    UnackedRpcResults results;
    TabletManager tabletManager;

    UnackedRpcResultsTest()
        : logEnabler()
        , context()
        , serverList(&context)
        , cluster(&context)
        , backup1Config(ServerConfig::forTesting())
        , backup1Id()
        , masterConfig(ServerConfig::forTesting())
        , service()
        , masterServer()
        , freer()
        , results(&context, &freer, NULL, &tabletManager)
        , tabletManager()
    {
        /////////////////////////////////
        // MasterService Initialization
        /////////////////////////////////
        uint32_t segmentSize = 256 * 1024;
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        backup1Config.localLocator = "mock:host=backup1";
        backup1Config.services = {WireFormat::BACKUP_SERVICE,
                WireFormat::ADMIN_SERVICE};
        backup1Config.segmentSize = segmentSize;
        backup1Config.backup.numSegmentFrames = 30;
        Server* server = cluster.addServer(backup1Config);
        server->backup->testingSkipCallerIdCheck = true;
        backup1Id = server->serverId;

        masterConfig = ServerConfig::forTesting();
        masterConfig.segmentSize = segmentSize;
        masterConfig.maxObjectDataSize = segmentSize / 4;
        masterConfig.localLocator = "mock:host=master";
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                WireFormat::ADMIN_SERVICE};
        masterConfig.master.logBytes = segmentSize * 30;
        masterConfig.master.numReplicas = 1;
        masterServer = cluster.addServer(masterConfig);
        service = masterServer->master.get();
        service->objectManager.log.sync();
        context.services[WireFormat::MASTER_SERVICE] = service;
        ////////////////////////////////////////
        // End of MasterService Initialization
        ////////////////////////////////////////

        results.leaseValidator = &service->clientLeaseValidator;

        void* result;
        ClientLease clientLease = {1, 1, 0};
        results.checkDuplicate(clientLease, 10, 5, &result);
        results.recordCompletion(1, 10, reinterpret_cast<void*>(1010), &freer);
    }

    DISALLOW_COPY_AND_ASSIGN(UnackedRpcResultsTest);
};

TEST_F(UnackedRpcResultsTest, checkDuplicate_basic) {
    void* result;
    ClientLease clientLease = {0, 0, 0};

    //1. New client
    clientLease = {2, 1, 0};
    EXPECT_FALSE(results.checkDuplicate(clientLease, 1, 0, &result));

    //2. Stale Rpc.
    clientLease = {1, 1, 0};
    EXPECT_THROW(results.checkDuplicate(clientLease, 4, 3, &result),
                 StaleRpcException);

    //3. Fast-path new RPC (rpcId > maxRpcId == true).
    EXPECT_EQ(10UL, results.clients[1]->maxRpcId);
    EXPECT_FALSE(results.checkDuplicate(clientLease, 11, 6, &result));
    EXPECT_EQ(0UL, (uint64_t)result);
    EXPECT_EQ(11UL, results.clients[1]->maxRpcId);
    EXPECT_EQ(6UL, results.clients[1]->maxAckId);

    EXPECT_TRUE(results.checkDuplicate(clientLease, 11, 6, &result));
    EXPECT_EQ(0UL, (uint64_t)result);

    //4. Duplicate RPC.
    EXPECT_TRUE(results.checkDuplicate(clientLease, 10, 6, &result));
    EXPECT_EQ(1010UL, (uint64_t)result);
    EXPECT_EQ(6UL, results.clients[1]->maxAckId);

    //5. Inside the window and new RPC.
    EXPECT_FALSE(results.checkDuplicate(clientLease, 9, 7, &result));
    EXPECT_EQ(0UL, (uint64_t)result);
    EXPECT_EQ(7UL, results.clients[1]->maxAckId);

    EXPECT_TRUE(results.checkDuplicate(clientLease, 9, 7, &result));
    EXPECT_EQ(0UL, (uint64_t)result);
}

TEST_F(UnackedRpcResultsTest, checkDuplicate_expiredLease) {
    void* result;
    ClientLease clientLease = {1, 1, 100};

    // Existing results should be returned.
    EXPECT_TRUE(results.checkDuplicate(clientLease, 10, 5, &result));
    EXPECT_EQ(1010UL, (uint64_t)result);

    // New requests should be rejected.
    EXPECT_THROW(results.checkDuplicate(clientLease, 11, 5, &result),
                 ExpiredLeaseException);
}

TEST_F(UnackedRpcResultsTest, checkDuplicate_validateWithUpdatedLease) {
    void* result;
    ClientLease clientLease = {1, 50, 1};
    EXPECT_FALSE(results.checkDuplicate(clientLease, 11, 5, &result));

    clientLease = {1, 1, 10};
    EXPECT_FALSE(results.checkDuplicate(clientLease, 12, 5, &result));
}

TEST_F(UnackedRpcResultsTest, shouldRecover) {
    //Basic Function
    EXPECT_TRUE(results.shouldRecover(1, 11, 5, LOG_ENTRY_TYPE_RPCRESULT));
    EXPECT_FALSE(results.shouldRecover(1, 5, 4, LOG_ENTRY_TYPE_RPCRESULT));

    //Duplicate RPCRESULT
    TestLog::reset();
    EXPECT_FALSE(results.shouldRecover(1, 10, 5, LOG_ENTRY_TYPE_RPCRESULT));
    EXPECT_EQ("shouldRecover: Duplicate Linearizable Rpc Record found during "
              "recovery. <clientID, rpcID, ackId> = <1, 10, 5>",
              TestLog::get());

    //Auto client insertion
    EXPECT_TRUE(results.shouldRecover(2, 4, 2, LOG_ENTRY_TYPE_RPCRESULT));
    // ^ ClientId = 2 inserted.
    std::unordered_map<uint64_t, UnackedRpcResults::Client*>::iterator it;
    it = results.clients.find(2);
    EXPECT_NE(it, results.clients.end());

    //Ack update
    UnackedRpcResults::Client* client = it->second;
    EXPECT_EQ(2UL, client->maxAckId);
}

TEST_F(UnackedRpcResultsTest, recordCompletion) {
    void* result;
    ClientLease clientLease = {1, 1, 0};
    EXPECT_FALSE(results.checkDuplicate(clientLease, 11, 5, &result));
    EXPECT_EQ(0UL, (uint64_t)result);
    results.recordCompletion(1, 11, reinterpret_cast<void*>(1011));
    EXPECT_TRUE(results.checkDuplicate(clientLease, 11, 5, &result));
    EXPECT_EQ(1011UL, (uint64_t)result);

    //Reusing spaces for acked rpcs.
    results.checkDuplicate(clientLease, 12, 5, &result);
    results.recordCompletion(1, 12, reinterpret_cast<void*>(1012));
    results.checkDuplicate(clientLease, 13, 5, &result);
    results.recordCompletion(1, 13, reinterpret_cast<void*>(1013));
    results.checkDuplicate(clientLease, 14, 10, &result);
    results.recordCompletion(1, 14, reinterpret_cast<void*>(1014));

    TestLog::Enable _;
    results.checkDuplicate(clientLease, 15, 11, &result); //Ack up to rpcId = 11
    EXPECT_EQ("freeLogEntry: freed <1011>", TestLog::get());

    results.recordCompletion(1, 15, reinterpret_cast<void*>(1015));
    results.checkDuplicate(clientLease, 16, 5, &result);
    results.recordCompletion(1, 16, reinterpret_cast<void*>(1016));

    //Ignore if acked and ignoreIfAck flag is set.
    results.recordCompletion(1, 4, reinterpret_cast<void*>(1012), true);
    results.recordCompletion(10, 1, reinterpret_cast<void*>(1012), true);

    EXPECT_EQ(16UL, results.clients[1]->maxRpcId);
    EXPECT_EQ(50, results.clients[1]->len);

    //Resized Client keeps the original data.
    results.checkDuplicate(clientLease, 17, 5, &result);
    EXPECT_EQ(50, results.clients[1]->len);
    for (int i = 12; i <= 16; ++i) {
        EXPECT_TRUE(results.checkDuplicate(clientLease, i, 5, &result));
        EXPECT_EQ((uint64_t)(i + 1000), (uint64_t)result);
    }
    EXPECT_TRUE(results.checkDuplicate(clientLease, 17, 5, &result));
    EXPECT_EQ(0UL, (uint64_t)result);

    results.recordCompletion(1, 17, reinterpret_cast<void*>(1017));
    EXPECT_TRUE(results.checkDuplicate(clientLease, 17, 5, &result));
    EXPECT_EQ(1017UL, (uint64_t)result);

    // Ack all RPCs appeared so far.
    TestLog::reset();
    clientLease = {1, 2, 0};
    results.checkDuplicate(clientLease, 18, 17, &result);
    EXPECT_EQ("freeLogEntry: freed <1012> | freeLogEntry: freed <1013> | "
              "freeLogEntry: freed <1014> | freeLogEntry: freed <1015> | "
              "freeLogEntry: freed <1016> | freeLogEntry: freed <1017>",
              TestLog::get());

    // If an RPC is already acked because client canceled it, recordCompletion()
    // should be no-op.
    results.checkDuplicate(clientLease, 19, 18, &result);
    results.recordCompletion(clientLease.leaseId,
                             18,
                             reinterpret_cast<void*>(1018));
    EXPECT_THROW(results.checkDuplicate(clientLease, 18, 17, &result),
                 StaleRpcException);
}

TEST_F(UnackedRpcResultsTest, recoverRecord) {
    TestLog::Enable _("recoverRecord");
    void* result;
    uint64_t leaseId = 10;

    UnackedRpcResults::ClientMap::iterator it = results.clients.find(leaseId);
    EXPECT_TRUE(it == results.clients.end());

    // New Record w/ rpcId or ackId updates.

    results.recoverRecord(leaseId, 20, 10, &result);

    it = results.clients.find(leaseId);
    EXPECT_FALSE(it == results.clients.end());
    EXPECT_EQ(10U, it->second->maxAckId);
    EXPECT_EQ(20U, it->second->maxRpcId);
    EXPECT_TRUE(it->second->hasRecord(20));

    // New Record w/o rpcId or ackId updates.
    EXPECT_FALSE(it->second->hasRecord(15));

    results.recoverRecord(leaseId, 15, 5, &result);

    it = results.clients.find(leaseId);
    EXPECT_FALSE(it == results.clients.end());
    EXPECT_EQ(10U, it->second->maxAckId);
    EXPECT_EQ(20U, it->second->maxRpcId);
    EXPECT_TRUE(it->second->hasRecord(15));

    // Unnecessary record.
    EXPECT_FALSE(it->second->hasRecord(5));

    results.recoverRecord(leaseId, 5, 1, &result);

    it = results.clients.find(leaseId);
    EXPECT_FALSE(it == results.clients.end());
    EXPECT_FALSE(it->second->hasRecord(5));

    // Duplicate record.
//    TestLog::reset();
    results.recoverRecord(leaseId, 15, 5, &result);
    EXPECT_EQ("recoverRecord: "
              "Duplicate RpcResult or ParticipantList found during recovery. "
              "<clientID, rpcID, ackId> = <10, 15, 5>",
            TestLog::get());
}

TEST_F(UnackedRpcResultsTest, isRpcAcked) {
    //1. Cleaned up client.
    EXPECT_TRUE(results.isRpcAcked(2, 1));

    //2. Existing client.
    EXPECT_TRUE(results.isRpcAcked(1, 5));
    EXPECT_FALSE(results.isRpcAcked(1, 6));
}

TEST_F(UnackedRpcResultsTest, SingleClientProtector) {
    uint64_t targetId = 42;
    uint64_t otherId = 84;

    // Dummy lock; to unsafely call internal methods this unit test
    std::mutex mutex;
    UnackedRpcResults::Lock dummyLock(mutex);

    UnackedRpcResults::SingleClientProtector* k2;
    UnackedRpcResults::Client* client = NULL;
    UnackedRpcResults::Client* otherClient = NULL;

    // Make sure targetClient doesn't exist
    client = results.getClientRecord(targetId, dummyLock);
    EXPECT_TRUE(client == NULL);
    {
        UnackedRpcResults::SingleClientProtector k0(&results, targetId);
        // k0 is live
        client = results.getClientRecord(targetId, dummyLock);
        EXPECT_TRUE(client != NULL);
        EXPECT_EQ(1, client->doNotRemove);
        {
            UnackedRpcResults::SingleClientProtector k1(&results, targetId);
            // k0, k1 is live
            EXPECT_EQ(2, client->doNotRemove);
            k2 = new UnackedRpcResults::SingleClientProtector(&results,
                                                              targetId);
            // k0, k1, k2 is live
            EXPECT_EQ(3, client->doNotRemove);
            {
                // Added unrelated KeepClientRecords object
                UnackedRpcResults::SingleClientProtector other(&results,
                                                               otherId);
                otherClient = results.getClientRecord(otherId, dummyLock);
                EXPECT_TRUE(otherClient != NULL);
                EXPECT_EQ(1, otherClient->doNotRemove);
                // Make sure main target didn't change.
                // k0, k1, k2 is live
                EXPECT_EQ(3, client->doNotRemove);
            }
            // Check other's destruction
            EXPECT_EQ(0, otherClient->doNotRemove);
            // Make sure main target didn't change.
            // k0, k1, k2 is live
            EXPECT_EQ(3, client->doNotRemove);
        }
        // k0, k2 is live
        EXPECT_EQ(2, client->doNotRemove);
        delete k2;
        // k0 is live
        EXPECT_EQ(1, client->doNotRemove);
    }
    // Nothing is live
    EXPECT_EQ(0, client->doNotRemove);
}

TEST_F(UnackedRpcResultsTest, cleanByTimeout_basic) {
    void* result;
    ClientLease clientLease = {0, 0, 0};
    results.cleanByTimeout();
    EXPECT_EQ(1U, results.clients.size());
    clientLease = {2, 1, 0};
    results.checkDuplicate(clientLease, 10, 5, &result);
    clientLease = {3, 1, 0};
    results.checkDuplicate(clientLease, 10, 5, &result);

    results.cleanByTimeout();
    EXPECT_EQ(3U, results.clients.size());

    TestLog::Enable _;
    TestLog::reset();
    clientLease = {2, 2, 0};
    results.checkDuplicate(clientLease, 11, 10, &result);
    EXPECT_EQ("processAck: client acked unfinished RPC with rpcId <10>",
              TestLog::get());

    service->clusterClock.updateClock(ClusterTime(2));

    results.cleanByTimeout();
    EXPECT_EQ(2U, results.clients.size());

    //Complete in progress rpcs and try cleanup again.
    results.recordCompletion(3, 10, &result);
    results.cleanByTimeout();
    EXPECT_EQ(1U, results.clients.size());

    EXPECT_EQ(ClusterTime(2U), service->clusterClock.getTime());

    // Mock coordinator returns valid lease and we just update leaseExpiration
    // for the locally expired lease (test ClientLease value: {1, 1, 0}.
    ClientLease realLease = CoordinatorClient::renewLease(&context, 1);
    EXPECT_NE(0U, realLease.leaseId);
    EXPECT_LE(2U, realLease.leaseExpiration);
    clientLease = {realLease.leaseId, 1, 0};
    results.checkDuplicate(clientLease, 10, 5, &result);
    results.recordCompletion(realLease.leaseId, 10, &result);
    EXPECT_EQ(2U, results.clients.size());
    results.cleanByTimeout();
    EXPECT_EQ(2U, results.clients.size());
}

TEST_F(UnackedRpcResultsTest, cleanByTimeout_client_doNotRemove) {
    void* result;
    // Add two more clients; should have total of 3.
    ClientLease clientLease = {0, 0, 0};
    clientLease = {2, 1, 0};
    results.checkDuplicate(clientLease, 10, 5, &result);
    results.recordCompletion(2, 10, &result);
    clientLease = {3, 1, 0};
    results.checkDuplicate(clientLease, 10, 5, &result);
    results.recordCompletion(3, 10, &result);
    EXPECT_EQ(3U, results.clients.size());

    service->clusterClock.updateClock(ClusterTime(2));

    {
        // With prevent client 2 from being cleaned.
        UnackedRpcResults::SingleClientProtector _(&results, 2);
        results.cleanByTimeout();
        EXPECT_EQ(1U, results.clients.size());
        EXPECT_TRUE(results.clients.find(2) != results.clients.end());
    }

    // Without the KeepClientRecord object, everything should be cleaned.
    results.cleanByTimeout();
    EXPECT_EQ(0U, results.clients.size());
}

TEST_F(UnackedRpcResultsTest, cleanByTimeout_TabletIsLoadingState) {
    void* result;
    // Add two more clients; should have total of 3.
    ClientLease clientLease = {0, 0, 0};
    clientLease = {2, 1, 0};
    results.checkDuplicate(clientLease, 10, 5, &result);
    results.recordCompletion(2, 10, &result);
    clientLease = {3, 1, 0};
    results.checkDuplicate(clientLease, 10, 5, &result);
    results.recordCompletion(3, 10, &result);
    EXPECT_EQ(3U, results.clients.size());

    service->clusterClock.updateClock(ClusterTime(2));

    // With a NOT_READY tablet, nothing should be cleaned.
    tabletManager.addTablet(0, 10, 20, TabletManager::NOT_READY);
    results.cleanByTimeout();
    EXPECT_EQ(3U, results.clients.size());

    // After deleting NOT_READY tablet, everything should be cleaned.
    tabletManager.deleteTablet(0, 10, 20);
    results.cleanByTimeout();
    EXPECT_EQ(0U, results.clients.size());
}

TEST_F(UnackedRpcResultsTest, hasRecord) {
    UnackedRpcResults::Client *client = results.clients[1];
    EXPECT_TRUE(client->hasRecord(10));
}

TEST_F(UnackedRpcResultsTest, result) {
    UnackedRpcResults::Client *client = results.clients[1];
    EXPECT_EQ(1010UL, (uint64_t)client->result(10));
}

TEST_F(UnackedRpcResultsTest, recordNewRpc) {
    UnackedRpcResults::Client *client = results.clients[1];
    client->recordNewRpc(11);
    EXPECT_TRUE(client->hasRecord(11));

    //Invoke resizing and test if all of the records are kept.
    int startRpcId = 12;
    int originalLen = client->len;
    for (int i = startRpcId; i < startRpcId + 2 * originalLen; ++i) {
        client->recordNewRpc(i);
    }
    EXPECT_NE(originalLen, client->len);
    for (int i = startRpcId; i < startRpcId + 2 * originalLen; ++i) {
        EXPECT_TRUE(client->hasRecord(i));
        EXPECT_EQ(0UL, (uint64_t)client->result(i));
    }
    EXPECT_EQ(1010UL, (uint64_t)client->result(10));
}

TEST_F(UnackedRpcResultsTest, recordNewRpc_jumResizeTest) {
    UnackedRpcResults::Client *client = results.clients[1];
    uint64_t rpcId1 = 11;
    client->recordNewRpc(rpcId1);
    EXPECT_TRUE(client->hasRecord(rpcId1));
    client->updateResult(rpcId1, reinterpret_cast<void*>(1011));

    //Invoke resizing and test if all of the records are kept.
    //Tests specifically for non-sequential increment of rpcIds.
    int originalLen = client->len;
    uint64_t rpcId2 = rpcId1 + 2 * originalLen;
    client->recordNewRpc(rpcId2);

    EXPECT_TRUE(client->hasRecord(rpcId1));
    EXPECT_TRUE(client->hasRecord(rpcId2));
    EXPECT_EQ(0UL, (uint64_t)client->result(rpcId2));
    EXPECT_EQ(1011UL, (uint64_t)client->result(rpcId1));
    EXPECT_NE(originalLen, client->len);
    EXPECT_EQ(1010UL, (uint64_t)client->result(10));
}

TEST_F(UnackedRpcResultsTest, resetRecord) {
    void* result;
    ClientLease clientLease = {1, 1, 0};
    results.checkDuplicate(clientLease, 18, 17, &result);
    EXPECT_TRUE(results.checkDuplicate(clientLease, 18, 17, &result));

    // After reset, the RPC ID = 18 should not be considered as a duplicate.
    results.resetRecord(clientLease.leaseId, 18);
    EXPECT_FALSE(results.checkDuplicate(clientLease, 18, 17, &result));
    EXPECT_TRUE(results.checkDuplicate(clientLease, 18, 17, &result));

    // If an RPC is already acked because client canceled it, resetRecord()
    // should be no-op.
    results.checkDuplicate(clientLease, 19, 18, &result);
    results.resetRecord(clientLease.leaseId, 18);
    EXPECT_THROW(results.checkDuplicate(clientLease, 18, 17, &result),
                 StaleRpcException);
}

TEST_F(UnackedRpcResultsTest, updateResult) {
    UnackedRpcResults::Client *client = results.clients[1];
    EXPECT_EQ(1010UL, (uint64_t)client->result(10));
    client->updateResult(10, reinterpret_cast<void*>(1099));
    EXPECT_EQ(1099UL, (uint64_t)client->result(10));

    client->recordNewRpc(11);
    EXPECT_EQ(0UL, (uint64_t)client->result(11));
    client->updateResult(11, reinterpret_cast<void*>(1011));
    EXPECT_EQ(1011UL, (uint64_t)client->result(11));
}

TEST_F(UnackedRpcResultsTest, getClientRecord) {
    UnackedRpcResults::Lock lock(results.mutex);

    EXPECT_TRUE(results.getClientRecord(42, lock) == NULL);

    UnackedRpcResults::Client* client =
            new UnackedRpcResults::Client(results.default_rpclist_size);
    results.clients[42] = client;

    EXPECT_TRUE(results.getClientRecord(42, lock) == client);
}

TEST_F(UnackedRpcResultsTest, getOrInitClientRecord) {
    UnackedRpcResults::Lock lock(results.mutex);

    EXPECT_TRUE(results.getClientRecord(42, lock) == NULL);

    UnackedRpcResults::Client* client = NULL;
    client = results.getOrInitClientRecord(42, lock);

    EXPECT_TRUE(client != NULL);

    EXPECT_TRUE(results.getClientRecord(42, lock) == client);
}

TEST_F(UnackedRpcResultsTest, unackedRpcHandle) {
    void* result;
    ClientLease clientLease = {1, 1, 0};
    { // 1. Normal use case test. No interruption between.
        UnackedRpcHandle urh(&results, clientLease, 11, 5);
        EXPECT_FALSE(urh.isDuplicate());
        EXPECT_EQ(0UL, urh.resultLoc());
        urh.recordCompletion(1011);
    }
    EXPECT_TRUE(results.checkDuplicate(clientLease, 11, 5, &result));
    EXPECT_EQ(1011UL, (uint64_t)result);

    { //2. Duplicate RPC exists.
        UnackedRpcHandle urh(&results, clientLease, 11, 5);
        EXPECT_TRUE(urh.isDuplicate());
        EXPECT_EQ(1011UL, urh.resultLoc());
    }
    { //3. In-progress RPC exists.
        {
            UnackedRpcHandle urh(&results, clientLease, 12, 5);
            EXPECT_FALSE(urh.isDuplicate());
            EXPECT_EQ(0UL, urh.resultLoc());

            UnackedRpcHandle urh2(&results, clientLease, 12, 5);
            EXPECT_TRUE(urh2.isDuplicate());
            EXPECT_TRUE(urh2.isInProgress());

            urh.recordCompletion(1012);
        }
        UnackedRpcHandle urh3(&results, clientLease, 12, 5);
        EXPECT_TRUE(urh3.isDuplicate());
        EXPECT_FALSE(urh3.isInProgress());
        EXPECT_EQ(1012UL, urh3.resultLoc());
    }
    { //4. Reset of record by out-of-scope.
        {
            UnackedRpcHandle urh(&results, clientLease, 13, 5);
            EXPECT_FALSE(urh.isDuplicate());
            EXPECT_EQ(0UL, urh.resultLoc());
        }
        UnackedRpcHandle urh2(&results, clientLease, 13, 5);
        EXPECT_FALSE(urh2.isDuplicate());
    }
    { //5. After recordCompletion, don't reset of record by out-of-scope.
        {
            UnackedRpcHandle urh(&results, clientLease, 14, 5);
            EXPECT_FALSE(urh.isDuplicate());
            EXPECT_EQ(0UL, urh.resultLoc());
            urh.recordCompletion(1014);
        }
        UnackedRpcHandle urh2(&results, clientLease, 14, 5);
        EXPECT_TRUE(urh2.isDuplicate());
        EXPECT_EQ(1014UL, urh2.resultLoc());
    }
}

}  // namespace RAMCloud
