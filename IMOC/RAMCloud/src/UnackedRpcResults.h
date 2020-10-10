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

#ifndef RAMCLOUD_UNACKEDRPCRESULTS_H
#define RAMCLOUD_UNACKEDRPCRESULTS_H

#include <unordered_map>
#include "AbstractLog.h"
#include "Common.h"
#include "ClientLeaseValidator.h"
#include "SpinLock.h"
#include "TabletManager.h"
#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * A temporary storage for results of linearizable RPCs that have not been
 * acknowledged by client.
 *
 * Each master should keep an instance of this class to keep the information
 * on which rpc has been processed with its result. The information should be
 * used to avoid processing re-tried RPCs again.
 */
class UnackedRpcResults {
  PUBLIC:
    explicit UnackedRpcResults(Context* context,
                               AbstractLog::ReferenceFreer* freer,
                               ClientLeaseValidator* leaseValidator,
                               TabletManager* tabletManager);
    ~UnackedRpcResults();
    void startCleaner();
    void resetFreer(AbstractLog::ReferenceFreer* freer);
    bool checkDuplicate(WireFormat::ClientLease clientLease,
                        uint64_t rpcId,
                        uint64_t ackId,
                        void** result);
    bool shouldRecover(uint64_t clientId,
                       uint64_t rpcId,
                       uint64_t ackId,
                       LogEntryType entryType);
    void recordCompletion(uint64_t clientId,
                             uint64_t rpcId,
                             void* result,
                             bool ignoreIfAcked = false);
    void recoverRecord(uint64_t clientId,
                         uint64_t rpcId,
                         uint64_t ackId,
                         void* result);
    void resetRecord(uint64_t clientId,
                     uint64_t rpcId);
    bool isRpcAcked(uint64_t clientId, uint64_t rpcId);

    /**
     * This class is used to prevent RPC Result records for a specified client
     * from being cleaned from the referenced UnackedRpcResults module.
     * Creating an SingleClientProtector object prevents cleaning of the
     * specified client's RPC Result records until the object is destroyed.
     * When multiple instances of this class are created, cleaning will not
     * resume until all instances have been deleted.
     *
     * This class does NOT prevent a client's normal case ACKs from removing
     * individual RPC results.
     *
     * Used by TransactionManger to ensure transaction related RPC results are
     * kept around even after the lease expires to ensure the results are
     * available for transaction recovery.
     */
    class SingleClientProtector {
      PUBLIC:
        SingleClientProtector(UnackedRpcResults* unackedRpcResults,
                              uint64_t clientId);
        ~SingleClientProtector();

      PRIVATE:
        // Keep reference to the unackedRpcResults so that it can be accessed
        // during the destruction of this object.
        UnackedRpcResults* unackedRpcResults;

        // Keep reference to the clientId in question so that it can be marked
        // "cleanable" when this object is destroyed.
        uint64_t clientId;

        DISALLOW_COPY_AND_ASSIGN(SingleClientProtector);
    };

  PRIVATE:
    void cleanByTimeout();
    /// Used only for testing.
    bool hasRecord(uint64_t clientId, uint64_t rpcId);

    /**
     * Holds info about outstanding RPCs, which is needed to avoid re-doing
     * the same RPC.
     */
    struct UnackedRpc {
        UnackedRpc(uint64_t rpcId = 0, void* resultPtr = 0)
            : id(rpcId), result(resultPtr) {}

        /**
         * Rpc id assigned by a client.
         */
        uint64_t id;

        /**
         * Log reference to the RpcResult log entry in log-structured memory.
         * This value may be NULL to indicate the RPC is still in progress.
         */
        void* result;
    };

    /**
     * The Cleaner periodically wakes up to clean up records of clients with
     * expired leases in unackedRpcResults.
     * Cleaner blocks the access to unackedRpcResults, so we limit the number
     * of clients we clean each time.
     */
    class Cleaner : public WorkerTimer {
      public:
        explicit Cleaner(UnackedRpcResults* unackedRpcResults);
        virtual void handleTimerEvent();

        /// The pointer to unackedRpcResults which will be cleaned.
        UnackedRpcResults* unackedRpcResults;

        /// Starting point of next round of cleaning.
        uint64_t nextClientToCheck;

        /// The maximum number of clients we check for liveness.
        static const int maxIterPerPeriod = 1000;
      private:
        DISALLOW_COPY_AND_ASSIGN(Cleaner);
    };

    /**
     * Each instance of this class stores information about unacknowledged RPCs
     * from a single client, which is determined by the client id.
     */
    class Client {
      PUBLIC:
        /**
         * Constructor for Client
         *
         * \param size
         *      Initial size of the array which keeps UnackedRpc.
         */
        explicit Client(int size)
            : maxRpcId(0)
            , maxAckId(0)
            , leaseExpiration()
            , numRpcsInProgress(0)
            , rpcs(new UnackedRpc[size]())
            , len(size)
            , doNotRemove(0)
        {}

        ~Client() {
            delete[] rpcs;
        }

        bool hasRecord(uint64_t rpcId);
        void* result(uint64_t rpcId);
        void recordNewRpc(uint64_t rpcId);
        void updateResult(uint64_t rpcId, void* result);
        void processAck(uint64_t ackId, AbstractLog::ReferenceFreer* freer);
        void resizeRpcs(int newLen);

        /**
         * Keeps the largest RpcId from the client seen in this master.
         * This is used to optimize checkDuplicate() in general case.
         */
        uint64_t maxRpcId;

        /**
         * Largest RpcId for which the client has acknowledged receiving
         * the result. We do not need to retain information for
         * RPCs with rpcId <= maxAckId.
        */
        uint64_t maxAckId;

        /**
         * A cluster clock value giving a hint about when the client's
         * lease expires (it definitely will not expire before this time,
         * but the lease may have been extended since this value was written).
         * Used in Cleaner for GC.
         */
        ClusterTime leaseExpiration;

        /**
         * The count for rpcIds in stage between checkDuplicate and
         * recordCompletion (aka. in Progress).
         * The count is used while cleanup to prevent removing client
         * with rpcs in progress.
         */
        int numRpcsInProgress;

        /**
         * Dynamically allocated array keeping #UnackedRpc of this client.
         * This keeps the status of each RPC until being acknowledged.
         * The array is initially with small size, let's say 5. Then, the info
         * about a rpc with id = i is recorded in (i % 5)th index of the array.
         *
         *   Example of array #Client::rpcs
         * | index  |  0  |  1  |  2  |  3  |  4  |
         * | rpc id |  5  | 11  |  7  |  8  |  9  |
         *
         * If the index (i % len), where #Client::len is the size of array, is
         * already occupied with other rpc which is not yet acknowledged, we
         * need to increase the size of array. But the maximum size of array
         * is bounded by the hard limit of the maximum number of outstanding
         * RPC by a client.
         */
        UnackedRpc* rpcs;

        /**
         * Length of the dynamic array, #rpcs.
         */
        int len;

        /**
         * Allows other modules to disallow or allow the removal for this client
         * record from UnackedRpcResutls by incrementing or decrementing this
         * value.  If this value is greater than zero, this record will not be
         * removed.  Otherwise, the cleaner is free to remove this record.
         */
        int doNotRemove;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(Client);
    };

    /**
     * Maps from a registered client ID to #Client.
     * Clients are dynamically allocated and must be freed explicitly.
     */
    typedef std::unordered_map<uint64_t, Client*> ClientMap;
    ClientMap clients;

    /**
     * Monitor-style lock. Any operation on internal data structure should
     * hold this lock.
     */
    std::mutex mutex;
    typedef std::lock_guard<std::mutex> Lock;

    /**
     * This value is used as initial array size of each Client instance.
     */
    int default_rpclist_size;

    Context* context;

    /**
     * Allows this module to determine if a lease is still valid.
     */
    ClientLeaseValidator* leaseValidator;

    Cleaner cleaner;

    /**
     * Pointer to reference freer. During garbage collection by ackId,
     * this freer should be used to designate specific log entry as
     * cleanable.
     */
    AbstractLog::ReferenceFreer* freer;

    /**
     * TabletManager allows the UnackedRpcResults cleaner to check whether
     * the master owns any tablet with NOT_READY state.
     * UnackedRpcResults uses this information while cleaning to prevent from
     * accidentally garbage collect RpcResults of an expired client before
     * the corresponding transaction to complete.
     */
    TabletManager* tabletManager;

    // Helper methods
    Client* getClientRecord(uint64_t clientId, Lock& lock);
    Client* getOrInitClientRecord(uint64_t clientId, Lock& lock);

    DISALLOW_COPY_AND_ASSIGN(UnackedRpcResults);
};

/**
 * A linearizable RPC handler should construct this class to check duplicate
 * RPC in progress and to record the result of RPC after processing.
 *
 * This handle should be used instead of directly calling UnackedRpcResults
 * for exception safety.
 *
 * Destruction of this class should happen only after log-sync.
 */
class UnackedRpcHandle {
  PUBLIC:
    UnackedRpcHandle(UnackedRpcResults* unackedRpcResults,
                     WireFormat::ClientLease clientLease,
                     uint64_t rpcId,
                     uint64_t ackId);
    UnackedRpcHandle(const UnackedRpcHandle& origin);
    UnackedRpcHandle& operator= (const UnackedRpcHandle& origin);
    ~UnackedRpcHandle();

    bool isDuplicate();
    bool isInProgress();
    uint64_t resultLoc();
    void recordCompletion(uint64_t result);

  PRIVATE:
    /// Save clientId and rpcId to be used for recordCompletion later.
    /// We do this double lookup to circumvent problem while resizing rpcs array
    uint64_t clientId;
    uint64_t rpcId;

    /// Keeps the outcome of checkDuplicate() call in constructor.
    bool duplicate;

    /// Indicates the location of saved result.
    /// If it is a duplicate RPC, obtained from checkDuplicate() in constructor.
    /// If it is a new RPC, saved by #UnackedRpcHandle::recordCompletion() call.
    uint64_t resultPtr;

    /// Pointer to current unackedRpcResults.
    UnackedRpcResults* rpcResults;
};

}  // namespace RAMCloud

#endif // RAMCLOUD_UNACKEDRPCRESULTS_H
