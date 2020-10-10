/* Copyright (c) 2012-2016 Stanford University
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

#ifndef RAMCLOUD_TABLEMANAGER_H
#define RAMCLOUD_TABLEMANAGER_H

#include <mutex>

#include "Common.h"
#include "CoordinatorUpdateManager.h"
#include "ServerId.h"
#include "Table.pb.h"
#include "Tablet.h"
#include "TableConfig.pb.h"
#include "Indexlet.h"

namespace RAMCloud {

/**
 * Used by the coordinator to map each tablet to the master that serves
 * requests for that tablet. The TableMap is the definitive truth about
 * tablet ownership in a cluster.
 *
 * Instances are locked for thread-safety, and methods return tablets
 * by-value to avoid inconsistencies due to concurrency.
 */
class TableManager {
  PUBLIC:

    /// Thrown if the given table does not exist.
    struct NoSuchTable : public Exception {
        explicit NoSuchTable(const CodeLocation& where)
                : Exception(where) {}
    };

    /**
     * Thrown from methods when the arguments indicate a tablet that is
     * not present in the tablet map.
     */
    struct NoSuchTablet : public Exception {
        explicit NoSuchTablet(const CodeLocation& where) : Exception(where) {}
    };

    /**
     * Thrown from methods when the arguments indicate a indexlet that is
     * not present in the indexlet map.
     */
    struct NoSuchIndexlet : public Exception {
        explicit NoSuchIndexlet(const CodeLocation& where) : Exception(where) {}
    };

    explicit TableManager(Context* context,
            CoordinatorUpdateManager* updateManager);
    ~TableManager();

    void coordSplitAndMigrateIndexlet(ServerId newOwner,
            uint64_t tableId, uint8_t indexId,
            const void* splitKey, KeyLength splitKeyLength);
    void createIndex(uint64_t tableId, uint8_t indexId, uint8_t indexType,
            uint8_t numIndexlets);
    uint64_t createTable(const char* name, uint32_t serverSpan,
            ServerId serverId = ServerId());
    string debugString(bool shortForm = false);
    void dropIndex(uint64_t tableId, uint8_t indexId);
    void dropTable(const char* name);
    uint64_t getTableId(const char* name);
    Tablet getTablet(uint64_t tableId, uint64_t keyHash);
    bool getIndexletInfoByBackingTableId(uint64_t backingTableId,
            ProtoBuf::Indexlet& indexletInfo);
    void indexletRecovered(uint64_t tableId, uint8_t indexId,
            void* firstKey, uint16_t firstKeyLength,
            void* firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
            ServerId serverId, uint64_t backingTableId);
    bool isIndexletTable(uint64_t tableId);
    vector<Tablet> markAllTabletsRecovering(ServerId serverId);
    void reassignTabletOwnership(ServerId newOwner, uint64_t tableId,
            uint64_t startKeyHash, uint64_t endKeyHash,
            uint64_t ctimeSegmentId, uint64_t ctimeSegmentOffset);
    void recover(uint64_t lastCompletedUpdate);
    void serializeTableConfig(ProtoBuf::TableConfig* tableConfig,
            uint64_t tableId);
    void splitTablet(const char* name, uint64_t splitKeyHash);
    void splitRecoveringTablet(uint64_t tableId, uint64_t splitKeyHash);
    void tabletRecovered(uint64_t tableId, uint64_t startKeyHash,
            uint64_t endKeyHash, ServerId serverId, LogPosition ctime);

  PRIVATE:
    /**
     * The following structure holds information about a indexlet of an index.
     * 
     * Each indexlet is stored by a backing RAMCloud table. The name of the
     * table is synthesized and has the format:
     * "__backingTable:tableId:indexId:i" where tableId and indexId
     * identify the index and i refers to the i-th indexlet corresponding
     * to that index.
     */
    struct Indexlet : public RAMCloud::Indexlet {
        public:
        Indexlet(const void *firstKey, uint16_t firstKeyLength,
                const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
                ServerId serverId, uint64_t backingTableId,
                uint64_t tableId, uint8_t indexId)
            : RAMCloud::Indexlet(firstKey, firstKeyLength, firstNotOwnedKey,
                       firstNotOwnedKeyLength)
            , serverId(serverId)
            , backingTableId(backingTableId)
            , tableId(tableId)
            , indexId(indexId)
        {}

        Indexlet(const Indexlet& indexlet)
            : RAMCloud::Indexlet(indexlet)
            , serverId(indexlet.serverId)
            , backingTableId(indexlet.backingTableId)
            , tableId(indexlet.tableId)
            , indexId(indexlet.indexId)
        {}

        /// The server id of the master owning this indexlet.
        ServerId serverId;

        /// The id of the backing table for the indexlet that is stored
        /// on the server with id serverId.
        uint64_t backingTableId;

        /// The id of the owning table
        uint64_t tableId;

        /// The id of the owning index
        uint8_t indexId;
    };

    /**
     * The following class holds information about a single index of a table.
     */
    struct Index {
        Index(uint64_t tableId, uint8_t indexId, uint8_t indexType)
            : tableId(tableId)
            , indexId(indexId)
            , indexType(indexType)
            , nextIndexletIdSuffix(0)
            , indexlets()
        {}
        ~Index();

        /// The id of the containing table.
        uint64_t tableId;

        /// Identifier used to refer to index within a table.
        uint8_t indexId;

        /// Type of the index.
        uint8_t indexType;

        /// Currently, the backingTable name for an indexlet is in the format
        /// "__backingTable:%lu:%d:%d", and nextIndexletIdSuffix indicates
        /// the next value to use for the last %d in that name.
        uint8_t nextIndexletIdSuffix;

        /// Information about each of the indexlets of index in the table. The
        /// entries are allocated and freed dynamically.
        vector<Indexlet*> indexlets;
    };

    /// An instance of this is a part of a Table and is used to store the
    /// information about each of the indexes for that table.
    /// It is a mapping between an indexId and the Index corresponding to that
    /// indexId for that table.
    typedef std::unordered_map<uint8_t, Index*> IndexMap;

    struct Table {
        Table(const char* name, uint64_t id)
            : name(name)
            , id(id)
            , tablets()
            , indexMap()
        {}
        ~Table();

        /// Human-readable name for the table (unique among all tables).
        string name;

        /// Identifier used to refer to the table in RPCs.
        uint64_t id;

        /// Information about each of the tablets in the table. The
        /// entries are allocated and freed dynamically.
        vector<Tablet*> tablets;

        /// Information about each of the indexes in the table. The
        /// entries are allocated and freed dynamically.
        IndexMap indexMap;
    };

    /**
     * Provides monitor-style protection for all operations on the tablet map.
     * A Lock for this mutex must be held to read or modify any state in
     * the tablet map.
     */
    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /// Shared information about the server.
    Context* context;

    /// Used for keeping track of updates on external storage.
    CoordinatorUpdateManager* updateManager;

    /// Id of the next table to be created.  Table ids are never reused.
    uint64_t nextTableId;

    /// Identifies the master to which we assigned the most recent tablet;
    /// used to rotate among the masters when assigning new tables.
    ServerId tabletMaster;

    /// Maps from a table name to information about that table. The table
    /// information is dynamically allocated (and shared with idMap).
    typedef std::unordered_map<string, Table*> Directory;
    Directory directory;

    /// Maps from a table id to information about that table. The table
    /// information is dynamically allocated (and shared with directory).
    typedef std::unordered_map<uint64_t, Table*> IdMap;
    IdMap idMap;

    /// Maps from backingTable id to indexlet.
    /// This is a map since every backingTable can have at most one table
    /// containing indexlet.
    typedef std::unordered_map<uint64_t, Indexlet*> IndexletTableMap;
    IndexletTableMap backingTableMap;

    uint64_t createTable(const Lock& lock, const char* name,
            uint32_t serverSpan, ServerId serverId = ServerId());
    void dropIndex(const Lock& lock, uint64_t tableId, uint8_t indexId);
    void dropTable(const Lock& lock, const char* name);
    TableManager::Indexlet* findIndexlet(const Lock& lock, Index* index,
            const void* key, uint16_t keyLength);
    Tablet* findTablet(const Lock& lock, Table* table, uint64_t keyHash);
    void notifyCreate(const Lock& lock, Table* table);
    void notifyCreateIndex(const Lock& lock, Index* index);
    void notifyDropTable(const Lock& lock, ProtoBuf::Table* info);
    void notifyDropIndex(const Lock& lock, Index* index);
    void notifySplitTablet(const Lock& lock, ProtoBuf::Table* info);
    void notifyReassignIndexlet(const Lock& lock, ProtoBuf::Table* info);
    void notifyReassignTablet(const Lock& lock, ProtoBuf::Table* info);
    Table* recreateTable(const Lock& lock, ProtoBuf::Table* info);
    void serializeTable(const Lock& lock, Table* table,
            ProtoBuf::Table* externalInfo);
    void syncNextTableId(const Lock& lock);
    void syncTable(const Lock& lock, Table* table,
            ProtoBuf::Table* externalInfo);
    void testAddTablet(const Tablet& tablet);
    void testCreateTable(const char* name, uint64_t id);
    Tablet* testFindTablet(uint64_t tableId, uint64_t keyHash);

    DISALLOW_COPY_AND_ASSIGN(TableManager);
};

} // namespace RAMCloud

#endif

