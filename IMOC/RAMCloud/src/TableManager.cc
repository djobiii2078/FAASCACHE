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

#include "CoordinatorServerList.h"
#include "CoordinatorService.h"
#include "IndexKey.h"
#include "Logger.h"
#include "MasterClient.h"
#include "ShortMacros.h"
#include "TableManager.h"
#include "TableManager.pb.h"

namespace RAMCloud {

/**
 * Construct a TableManager.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param updateManager
 *      Used for managing update information on external storage.
 */
TableManager::TableManager(Context* context,
        CoordinatorUpdateManager* updateManager)
    : mutex()
    , context(context)
    , updateManager(updateManager)
    , nextTableId(1)
    , tabletMaster()
    , directory()
    , idMap()
    , backingTableMap()
{
    context->tableManager = this;
}

/**
 * Destructor for TableManager.
 */
TableManager::~TableManager()
{
    // Must free all Table storage.
    for (Directory::const_iterator it = directory.begin();
            it != directory.end(); ++it) {
        Table* table = it->second;
        delete table;
    }
}

/**
 * Destructor for Table: must free all the Tablet structures.
 */
TableManager::Table::~Table()
{
    foreach (Tablet* tablet, tablets) {
        delete tablet;
    }

    for (IndexMap::const_iterator it = indexMap.begin();
            it != indexMap.end(); ++it){
        Index* index = it->second;
        delete index;
    }
}

/**
 * Destructor for Index: must free all the Indexlets structures.
 */
TableManager::Index::~Index()
{
    foreach (Indexlet* indexlet, indexlets) {
        delete indexlet;
    }
}

//////////////////////////////////////////////////////////////////////
// TableManager Public Methods
//////////////////////////////////////////////////////////////////////

/**
 * Split an indexlet into two disjoint indexlets at a specific key.
 * Check if the split already exists, in which case, just return.
 * Ask the original server to migrate the second indexlet resulting from the
 * split to another master having server id newOwner.
 * Alert newOwner that it should begin servicing requests on that indexlet.
 *
 * \param newOwner
 *      ServerId of the server that will own the second indexlet resulting
 *      from the split of the original indexlet at the end of the operation.
 * \param tableId
 *      Id of the table to which the index belongs.
 * \param indexId
 *      Id of the secondary key for which this index stores information.
 * \param splitKey
 *      Key used to partition the indexlet into two. Keys less than
 *      \a splitKey belong to one indexlet, keys greater than or equal to
 *      \a splitKey belong to the other.
 * \param splitKeyLength
 *      Length of splitKey in bytes.
 * 
 * \throw NoSuchIndexlet
 *      If the indexlet being split, or the index for which the indexlet
 *      is being split doesn't exist anymore.
 * \throw NoSuchTable
 *      If tableId does not specify an existing table.
 */
void
TableManager::coordSplitAndMigrateIndexlet(
        ServerId newOwner, uint64_t tableId, uint8_t indexId,
        const void* splitKey, KeyLength splitKeyLength)
{
    Lock lock(mutex);

    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end())
        throw NoSuchTable(HERE);
    Table* table = it->second;

    IndexMap::iterator indexIter = table->indexMap.find(indexId);
    if (indexIter == table->indexMap.end())
        throw NoSuchIndexlet(HERE);
    Index* index = indexIter->second;

    TableManager::Indexlet* indexlet =
            findIndexlet(lock, index, splitKey, splitKeyLength);

    if (indexlet == NULL)
        throw NoSuchIndexlet(HERE);

    if (IndexKey::keyCompare(splitKey, splitKeyLength,
            indexlet->firstKey, indexlet->firstKeyLength) == 0)
        return;

    // Save the firstNotOwnedKey from the original indexlet as we will
    // end up changing this information in the original indexlet.
    uint16_t firstNotOwnedKeyLength = indexlet->firstNotOwnedKeyLength;
    void* firstNotOwnedKey = malloc(firstNotOwnedKeyLength);
    memcpy(firstNotOwnedKey, indexlet->firstNotOwnedKey,
            firstNotOwnedKeyLength);

    // Create a backing table where the new master can store data for the
    // indexlet being migrated to it.
    string newBackingTableName;
    newBackingTableName.append(format("__backingTable:%lu:%d:%d",
            index->tableId, index->indexId, index->nextIndexletIdSuffix++));
    uint64_t newBackingTableId =
            createTable(lock, newBackingTableName.c_str(), 1, newOwner);
    assert(idMap.count(newBackingTableId) > 0U);

    MasterClient::prepForIndexletMigration(
            context, newOwner, tableId, indexId, newBackingTableId,
            splitKey, splitKeyLength,
            firstNotOwnedKey, firstNotOwnedKeyLength);

    MasterClient::splitAndMigrateIndexlet(
            context, indexlet->serverId, newOwner, tableId, indexId,
            indexlet->backingTableId, newBackingTableId,
            splitKey, splitKeyLength);

    // Perform the split on our in-memory structure and show that the
    // second indexlet is now a part of the new server.

    indexlet->firstNotOwnedKeyLength = splitKeyLength;
    free(indexlet->firstNotOwnedKey);
    indexlet->firstNotOwnedKey = malloc(splitKeyLength);
    memcpy(indexlet->firstNotOwnedKey, splitKey, splitKeyLength);

    index->indexlets.push_back(new Indexlet(
            splitKey, splitKeyLength, firstNotOwnedKey, firstNotOwnedKeyLength,
            newOwner, newBackingTableId, tableId, indexId));

    MasterClient::takeIndexletOwnership(
            context, newOwner, tableId, indexId, newBackingTableId,
            splitKey, splitKeyLength, firstNotOwnedKey, firstNotOwnedKeyLength);

    // TODO(syang0): Put in calls to trimAndBalance for both indexlets
    // once that is implemented.
}

/**
 * Create an index for table, if it doesn't already exist.
 *
 * \param tableId
 *      Id of the table to which the index belongs.
 * \param indexId
 *      Id of the secondary key on which the index is being built.
 * \param indexType
 *      Type of the keys corresponding to this index.
 *      Currently only string keys are supported, so this parameter is not yet
 *      used (i.e., caller can provide any value).
 * \param numIndexlets
 *      Number of indexlets to partition the index key space.
 *      This is only for performance testing, and value should always be 1 for
 *      real use.
 *
 * \throw NoSuchTable
 *      If tableId does not specify an existing table.
 * \throw NoSuchTablet
 *      If the backing tablet of the index is not created properly.
 */
void
TableManager::createIndex(uint64_t tableId, uint8_t indexId, uint8_t indexType,
        uint8_t numIndexlets)
{
    if (indexId == 0) {
        RAMCLOUD_LOG(NOTICE, "Invalid index id %u. Secondary keys have "
                             "index id greater than 0.", indexId);
        throw InvalidParameterException(HERE);
    }

    Lock lock(mutex);

    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end()) {
        RAMCLOUD_LOG(NOTICE, "Cannot find table '%lu'", tableId);
        throw NoSuchTable(HERE);
    }

    Table* table = it->second;

    // Search if the index already exists for the given table.
    IndexMap::iterator iit = table->indexMap.find(indexId);
    if (iit != table->indexMap.end()) {
        RAMCLOUD_LOG(NOTICE, "Index %u for table '%lu' already exists",
                     indexId, tableId);
        return;
    }

    LOG(NOTICE, "Creating index '%u' for table '%lu'", indexId, tableId);

    Index* index = new Index(tableId, indexId, indexType);
    try {
        for (index->nextIndexletIdSuffix = 0;
                index->nextIndexletIdSuffix < numIndexlets;
                (index->nextIndexletIdSuffix)++) {
            string backingTableName;
            backingTableName.append(format("__backingTable:%lu:%d:%d",
                    tableId, indexId, index->nextIndexletIdSuffix));
            // Create the backing table for indexlet.
            uint64_t backingTableId =
                    createTable(lock, backingTableName.c_str(), 1);

            IdMap::iterator itd = idMap.find(backingTableId);
            assert(itd != idMap.end());

            // Use the backingTable serverId to assign the indexlet.
            Tablet* backingTablet = findTablet(lock, itd->second, 0UL);

            if ((backingTablet->startKeyHash != 0UL)
                 || (backingTablet->endKeyHash != ~0UL)) {
                throw NoSuchTablet(HERE);
            }
            tabletMaster = backingTablet->serverId;

            Indexlet *indexlet;
            if (numIndexlets == 1) {
                char firstKey = 0;
                char firstNotOwnedKey = 127;
                indexlet = new Indexlet(
                        reinterpret_cast<void *>(&firstKey),
                        1, reinterpret_cast<void *>(&firstNotOwnedKey),
                        1, tabletMaster, backingTableId,
                        tableId, indexId);
            } else {
                // This case exists only for performance and unit testing, and
                // it is not intended for actual use.
                char firstKey =
                        static_cast<char>('a' + index->nextIndexletIdSuffix);
                char firstNotOwnedKey =
                        static_cast<char>('b' + index->nextIndexletIdSuffix);
                indexlet = new Indexlet(
                        reinterpret_cast<void *>(&firstKey),
                        1, reinterpret_cast<void *>(&firstNotOwnedKey),
                        1, tabletMaster, backingTableId,
                        tableId, indexId);
            }
            index->indexlets.push_back(indexlet);

            // Now we add tableIndexId<->indexlet into backingTableMap.
            backingTableMap.insert(
                std::make_pair(backingTableId, indexlet));
        }
    } catch (...) {
        delete index;
        throw;
    }

    table->indexMap[indexId] = index;
    notifyCreateIndex(lock, index);
    return;
}

/**
 * Create a table with the given name, if it doesn't already exist.
 *
 * \param name
 *      Name for the table to be created.
 * \param serverSpan
 *      Number of servers across which this table should be split during
 *      creation.
 * \param serverId
 *      Id of the server on which to locate all tablets for this table.
 *
 * \return
 *      Table id of the new table. If a table already exists with the
 *      given name, then its id is returned.
 */
uint64_t
TableManager::createTable(const char* name, uint32_t serverSpan,
        ServerId serverId)
{
    Lock lock(mutex);
    return createTable(lock, name, serverSpan, serverId);
}

/**
 * Returns a human-readable string describing all of the tables currently
 * in the tablet map. Used primarily for testing.
 *
 * \param shortForm
 *      If true, the output is abbreviated to just essential information
 *      (intended to minimize test breakage that occurs when someone changes
 *      behavior unrelated to a particular test).
 */
string
TableManager::debugString(bool shortForm)
{
    Lock lock(mutex);
    string result;
    for (uint64_t i = 0; i < nextTableId; ++i) {
        IdMap::iterator it = idMap.find(i);
        if (it == idMap.end()) {
            continue;
        }
        Table* table = it-> second;
        if (!result.empty()) {
            result += " ";
        }
        if (shortForm) {
            result += format("{ %s(id %lu):", table->name.c_str(), table->id);
        } else {
            result += format("Table { name: %s, id %lu,", table->name.c_str(),
                    table->id);
        }
        foreach (Tablet* tablet, table->tablets) {
            if (shortForm) {
                result += format(" { 0x%lx-0x%lx on %s }",
                        tablet->startKeyHash, tablet->endKeyHash,
                        tablet->serverId.toString().c_str());
            } else {
                const char* status = "NORMAL";
                if (tablet->status != Tablet::NORMAL)
                    status = "RECOVERING";
                result += format(" Tablet { startKeyHash: 0x%lx, "
                        "endKeyHash: 0x%lx, serverId: %s, status: %s, "
                        "ctime: %ld.%d }",
                        tablet->startKeyHash, tablet->endKeyHash,
                        tablet->serverId.toString().c_str(), status,
                        tablet->ctime.getSegmentId(),
                        tablet->ctime.getSegmentOffset());
            }
        }
        result += " }";
    }
    return result;
}

/**
 * Delete the table with the given name. All existing data for the table will
 * be deleted, and the table's name will no longer exist in the directory
 * of tables.
 * 
 * \param name
 *      Name of the table that is to be dropped.
 */
void
TableManager::dropTable(const char* name)
{
    Lock lock(mutex);
    dropTable(lock, name);
}

/**
 * All existing index data for the index will be deleted, but key values will
 * remain in objects of the table.
 *
 * \param tableId
 *      Id of the table to which the index belongs
 * \param indexId
 *      Id of the secondary key on which the index is being built
 */
void
TableManager::dropIndex(uint64_t tableId, uint8_t indexId)
{
    Lock lock(mutex);
    dropIndex(lock, tableId, indexId);
}

/**
 * Return the tableId of the table with the given name.
 *
 * \param name
 *      Name to identify the table.
 *
 * \return
 *      tableId of the table whose name is given.
 *
 * \throw NoSuchTable
 *      If name does not identify an existing table.
 */
uint64_t
TableManager::getTableId(const char* name)
{
    Lock lock(mutex);
    Directory::iterator it = directory.find(name);
    if (it != directory.end())
        return it->second->id;
    throw NoSuchTable(HERE);
}

/**
 * Get the details of a Tablet in the tablet map. This method is used
 * primarily for testing.
 *
 * \param tableId
 *      Identifier of the table containing the desired tablet.
 * \param keyHash
 *      Of the desired tablet is the one containing this key hash.
 * \return
 *      A copy of the Tablet entry in the tablet map corresponding to
 *      \a tableId and \a keyHash.
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 */
Tablet
TableManager::getTablet(uint64_t tableId, uint64_t keyHash)
{
    Lock lock(mutex);
    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end())
        throw NoSuchTablet(HERE);
    Table* table = it->second;
    return *findTablet(lock, table, keyHash);
}

/**
 * Return information about a indexlet (e.g., its key, tableId, indexId,
 * ServerId, and backingTableId), when given a backingTableId
 *
 * \param backingTableId
 *      Id of the backing table that will hold objects for this indexlet.
 * \param indexlet
 *      Indexlet information structure to populate
 * \return
 *      True if the querying table is an indexlet table.
 *      Otherwise, return false.
 */
bool
TableManager::getIndexletInfoByBackingTableId(uint64_t backingTableId,
        ProtoBuf::Indexlet& indexlet)
{
    Lock lock(mutex);
    IndexletTableMap::iterator it = backingTableMap.find(backingTableId);
    if (it == backingTableMap.end())
        return false;

    if (it->second->firstKey != NULL)
        indexlet.set_first_key(
                string(reinterpret_cast<char*>(it->second->firstKey),
                it->second->firstKeyLength));
    else
        indexlet.set_first_key("");

    if (it->second->firstNotOwnedKey != NULL)
        indexlet.set_first_not_owned_key(
                string(reinterpret_cast<char*>(it->second->firstNotOwnedKey),
                it->second->firstNotOwnedKeyLength));
    else
        indexlet.set_first_not_owned_key("");
    indexlet.set_table_id(it->second->tableId);
    indexlet.set_index_id(it->second->indexId);
    indexlet.set_backing_table_id(it->second->backingTableId);
    indexlet.set_server_id(it->second->serverId.getId());
    return true;
}

/**
 * Invoked by MasterRecoveryManager after recovery for a indexlet has
 * successfully completed to inform coordinator about the new master
 * for the indexlet.
 *
 * \param tableId
 *      Id of table to which this index belongs.
 * \param indexId
 *      Id of the secondary key for which this indexlet stores information.
 * \param firstKey
 *      Key blob marking the start of the indexed key range for this indexlet.
 * \param firstKeyLength
 *      Length of firstKeyStr.
 * \param firstNotOwnedKey
 *      Blob of the smallest key in the given index that is after firstKey
 *      in the index order but not part of this indexlet.
 * \param firstNotOwnedKeyLength
 *      Length of firstNotOwnedKey.
 * \param serverId
 *      Indexlet is updated to indicate that it is owned by \a serverId.
 * \param backingTableId
 *      Id of the backing table that will hold objects for this indexlet.
 *
 * \throw NoSuchIndexlet
 *      If the arguments do not identify a indexlet currently in the indexlet
 *      map.
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 */
void
TableManager::indexletRecovered(
        uint64_t tableId, uint8_t indexId, void* firstKey,
        uint16_t firstKeyLength, void* firstNotOwnedKey,
        uint16_t firstNotOwnedKeyLength, ServerId serverId,
        uint64_t backingTableId)
{
    Lock lock(mutex);

    // Find the desired table and indexlet.
    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end())
        throw NoSuchTablet(HERE);
    Table* table = it->second;
    if (!table->indexMap[indexId]) {
        throw NoSuchIndexlet(HERE);
    }
    Index* index = table->indexMap[indexId];

    bool foundIndexlet = 0;
    foreach (Indexlet* indexlet, index->indexlets) {
        if ((indexlet->firstKeyLength == firstKeyLength)
           &&(indexlet->firstNotOwnedKeyLength == firstNotOwnedKeyLength)
           &&(bcmp(indexlet->firstKey, firstKey, firstKeyLength) == 0)
           &&(bcmp(indexlet->firstNotOwnedKey, firstNotOwnedKey,
                   firstNotOwnedKeyLength) == 0))
        {
            indexlet->serverId = serverId;
            indexlet->backingTableId = backingTableId;
            foundIndexlet = 1;
            LOG(NOTICE, "found indexlet and changed its server id to %s",
                serverId.toString().c_str());
        }
    }
    if (!foundIndexlet) {
        LOG(NOTICE, "not found indexlet, which is an error");
    }
}

/**
 * Return if a table is indexlet table or not
 *
 * \param tableId
 *      Identifier of the table
 * \return
 *      True if the querying table is an index table. Otherwise, return false.
 */
bool
TableManager::isIndexletTable(uint64_t tableId)
{
    Lock lock(mutex);
    return backingTableMap.find(tableId) != backingTableMap.end();
}

/**
 * Update the status of all the Tablets in the tablet map that are on a
 * specific server as recovering, and return information about all of the
 * tablets.
 *
 * \param serverId
 *      Identifies the server whose tablets status should be marked as
 *      recovering.
 * \return
 *      Copies of all the Tablets that are owned by \a serverId.
 */
vector<Tablet>
TableManager::markAllTabletsRecovering(ServerId serverId)
{
    Lock lock(mutex);
    vector<Tablet> results;
    for (Directory::iterator it = directory.begin(); it != directory.end();
            ++it) {
        Table* table = it->second;
        foreach (Tablet* tablet, table->tablets) {
            if (tablet->serverId == serverId) {
                tablet->status = Tablet::RECOVERING;
                results.push_back(*tablet);
            }
        }
    }
    return results;
}

/**
 * Switch ownership of a tablet from one master to another and alert the new
 * master that it should begin servicing requests on that tablet. This method
 * does not actually transfer the contents of the tablet; the caller should
 * already have taken care of that. This method is used to complete the
 * migration of a tablet from one server to another.
 *
 * \param newOwner
 *      ServerId of the server that will own this tablet at the end of
 *      the operation.
 * \param tableId
 *      Table id of the tablet whose ownership is being reassigned.
 * \param startKeyHash
 *      First key hash that is part of the range of key hashes for the tablet.
 * \param endKeyHash
 *      Last key hash that is part of the range of key hashes for the tablet.
 * \param ctimeSegmentId
 *      ServerId of the log head before migration.
 * \param ctimeSegmentOffset
 *      Offset in log head before migration.
 *
 * \throw NoSuchTable
 *      If tableId does not specify an existing table.
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 */
void
TableManager::reassignTabletOwnership(
        ServerId newOwner, uint64_t tableId,
        uint64_t startKeyHash, uint64_t endKeyHash,
        uint64_t ctimeSegmentId, uint64_t ctimeSegmentOffset)
{
    Lock lock(mutex);
    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end())
        throw NoSuchTable(HERE);
    Table* table = it->second;
    Tablet* tablet = findTablet(lock, table, startKeyHash);
    if ((tablet->startKeyHash != startKeyHash)
            || (tablet->endKeyHash != endKeyHash))
        throw NoSuchTablet(HERE);
    if (tablet->serverId == newOwner) {
        RAMCLOUD_LOG(NOTICE, "Ownership of tablet [0x%lx,0x%lx] in tableId %lu"
                " already transfered", startKeyHash, endKeyHash, tableId);
        return;
    }

    LOG(NOTICE, "Reassigning tablet [0x%lx,0x%lx] in tableId %lu "
        "from %s to %s",
        startKeyHash, endKeyHash, tableId,
        context->coordinatorServerList->toString(tablet->serverId).c_str(),
        context->coordinatorServerList->toString(newOwner).c_str());

    // Get current head of log to preclude all previous data in the log
    // from being considered part of this tablet.
    LogPosition headOfLogAtCreation(ctimeSegmentId,
                                      ctimeSegmentOffset);
    tablet->ctime = headOfLogAtCreation;
    tablet->serverId = newOwner;
    tablet->status = Tablet::NORMAL;

    // Record information about the new assignment in external storage,
    // in case we crash.
    ProtoBuf::Table externalInfo;
    serializeTable(lock, table, &externalInfo);
    externalInfo.set_sequence_number(updateManager->nextSequenceNumber());
    ProtoBuf::Table::Reassign* reassign = externalInfo.mutable_reassign();
    reassign->set_server_id(newOwner.getId());
    reassign->set_start_key_hash(startKeyHash);
    reassign->set_end_key_hash(endKeyHash);
    syncTable(lock, table, &externalInfo);

    // Finish up by notifying the relevant master.
    notifyReassignTablet(lock, &externalInfo);
    updateManager->updateFinished(externalInfo.sequence_number());
}

/**
 * This method is called shortly after the coordinator assumes leadership
 * of the cluster; it recovers all of the table metadata from external
 * storage, and it completes any operations that might not have finished
 * at the time the previous coordinator crashed.
 *
 * \param lastCompletedUpdate
 *      Sequence number of the last update from the previous coordinator
 *      that is known to have finished. Any updates after this may or may
 *      not have finished, so we must do whatever is needed to complete
 *      them.
 */
void
TableManager::recover(uint64_t lastCompletedUpdate)
{
    Lock lock(mutex);

    // Restore overall state information.
    ProtoBuf::TableManager info;
    if (context->externalStorage->getProtoBuf("tableManager", &info)) {
        nextTableId = info.next_table_id();
        RAMCLOUD_LOG(NOTICE, "initializing TableManager: nextTableId = %lu",
                nextTableId);
    }

    // Fetch all of the table-related information from external storage.
    vector<ExternalStorage::Object> objects;
    context->externalStorage->getChildren("tables", &objects);

    // Each iteration through the following loop processes the metadata
    // for one table.
    foreach (ExternalStorage::Object& object, objects) {
        // First, parse the protocol buffer containing the table's metadata
        if (object.value == NULL)
            continue;
        ProtoBuf::Table info;
        string str(object.value, object.length);
        if (!info.ParseFromString(str)) {
            throw FatalError(HERE, format(
                    "couldn't parse protocol buffer in /tables/%s",
                    object.name));
        }

        // Update the nextTableId if this table has a higher id than
        // any id we have encountered so far.
        if (info.id() >= nextTableId)
            nextTableId = info.id() + 1;

        // Regenerate our internal information for the table, unless the
        // table has been deleted.
        Table* table = NULL;
        if (!info.has_deleted()) {
            table = recreateTable(lock, &info);
        }

        if (info.sequence_number() <= lastCompletedUpdate) {
            // There is no additional cleanup to do for this table.
            continue;
        }

        // The last metadata update for this table may not have completed.
        // Check for each possible update and clean up appropriately.
        if (info.has_created()) {
            notifyCreate(lock, table);
        }
        if (info.has_deleted()) {
            notifyDropTable(lock, &info);
        }
        if (info.has_split()) {
            notifySplitTablet(lock, &info);
        }
        if (info.has_reassign()) {
            notifyReassignTablet(lock, &info);
        }
    }
    LOG(NOTICE, "Table recovery complete: %lu table(s)", directory.size());
}

/**
 * Fills in a protocol buffer with information describing which masters store
 * which pieces of data for a given table (including both tablets and indexes).
 * \param tableConfig
 *      Protocol buffer to which entries are added representing each of
 *      the tablets and indexes for a given table.
 * \param tableId
 *      The id of the table whose configuration will be fetched. If
 *      the table doesn't exist, then the protocol buffer ends up empty.
 */
void
TableManager::serializeTableConfig(ProtoBuf::TableConfig* tableConfig,
        uint64_t tableId)
{
    Lock lock(mutex);
    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end())
        return;
    Table* table = it->second;

    // filling tablets
    foreach (Tablet* tablet, table->tablets) {
        ProtoBuf::TableConfig::Tablet& entry(*tableConfig->add_tablet());
        tablet->serialize((ProtoBuf::Tablets::Tablet&)entry);
        try {
            string locator = context->serverList->getLocator(
                    tablet->serverId);
            entry.set_service_locator(locator);
        } catch (const ServerListException& e) {
            RAMCLOUD_CLOG(NOTICE, "Server id (%s) in tablet map no longer "
                    "in server list; omitting locator for entry (tableName %s, "
                    "tableId %lu, startKeyHash 0x%lx)",
                    tablet->serverId.toString().c_str(), table->name.c_str(),
                    tableId, tablet->startKeyHash);
        }
    }

    // filling indexes
    for (IndexMap::const_iterator iit = table->indexMap.begin();
            iit != table->indexMap.end(); ++iit) {
        Index* index = iit->second;
        if (index == NULL)
            continue;

        ProtoBuf::TableConfig::Index& index_entry(*tableConfig->add_index());
        index_entry.set_index_id(index->indexId);
        index_entry.set_index_type(index->indexType);

        // filling indexlets
        foreach (Indexlet* indexlet, index->indexlets) {
            ProtoBuf::TableConfig::Index::Indexlet&
                   entry(*index_entry.add_indexlet());
            if (indexlet->firstKey != NULL) {
                entry.set_start_key(string(
                        reinterpret_cast<char*>(indexlet->firstKey),
                        indexlet->firstKeyLength));
            } else {
                entry.set_start_key("");
            }

            if (indexlet->firstNotOwnedKey != NULL) {
                entry.set_end_key(string(
                        reinterpret_cast<char*>(indexlet->firstNotOwnedKey),
                        indexlet->firstNotOwnedKeyLength));
            } else {
                entry.set_end_key("");
            }

            entry.set_server_id(indexlet->serverId.getId());
            try {
                string locator = context->serverList->getLocator(
                        indexlet->serverId);
                entry.set_service_locator(locator);
            } catch (const ServerListException& e) {
                RAMCLOUD_LOG(NOTICE, "Server id (%s) in index map no longer in "
                    "server list; omitting locator for entry (tableName %s,"
                    "tableId %lu, indexId %d)",
                    indexlet->serverId.toString().c_str(), table->name.c_str(),
                    tableId, index->indexId);
            }
        }
    }
}

/**
 * Split a tablet into two disjoint tablets at a specific key hash. Check
 * if the split already exists, in which case, just return. Also informs
 * the master to split the tablet.
 *
 * \param name
 *      Name of the table that contains the tablet to be split.
 * \param splitKeyHash
 *      Key hash to used to partition the tablet into two. Keys less than
 *      \a splitKeyHash belong to one tablet, keys greater than or equal to
 *      \a splitKeyHash belong to the other.
 *
 * \throw NoSuchTable
 *      If name does not correspond to an existing table.
 */
void
TableManager::splitTablet(const char* name, uint64_t splitKeyHash)
{
    Lock lock(mutex);
    Directory::iterator it = directory.find(name);
    if (it == directory.end())
        throw NoSuchTable(HERE);
    Table* table = it->second;
    Tablet* tablet = findTablet(lock, table, splitKeyHash);
    if (splitKeyHash == tablet->startKeyHash)
        return;
    if (tablet->status == Tablet::RECOVERING) {
        // We can't process this request right now, because recovery may
        // undo it. Try again when recovery is finished.
        throw RetryException(HERE, 1000000, 2000000,
                "can't split tablet now: recovery is underway");
    }

    // Perform the split on our in-memory structures.
    table->tablets.push_back(new Tablet(tablet->tableId, splitKeyHash,
            tablet->endKeyHash, tablet->serverId, tablet->status,
            tablet->ctime));
    tablet->endKeyHash = splitKeyHash - 1;

    // Record information about the split in external storage, in case we
    // crash.
    ProtoBuf::Table externalInfo;
    serializeTable(lock, table, &externalInfo);
    externalInfo.set_sequence_number(updateManager->nextSequenceNumber());
    ProtoBuf::Table::Split* split = externalInfo.mutable_split();
    split->set_server_id(tablet->serverId.getId());
    split->set_split_key_hash(splitKeyHash);
    syncTable(lock, table, &externalInfo);

    // Finish up by notifying the relevant master.
    notifySplitTablet(lock, &externalInfo);
    updateManager->updateFinished(externalInfo.sequence_number());
}

/**
 * This method is similar to splitTablet, except that it is only
 * invoked for a tablet owned by a master being recovered. This
 * results in slightly different functionality (e.g., no need to
 * notify the master).
 *
 * \param tableId
 *      Id of the table that contains the tablet to be split.
 * \param splitKeyHash
 *      Key hash to used to partition the tablet into two. Keys less than
 *      \a splitKeyHash belong to one tablet, keys greater than or equal to
 *      \a splitKeyHash belong to the other.
 *
 * \throw NoSuchTable
 *      If tableId does not specify an existing table.
 */
void
TableManager::splitRecoveringTablet(uint64_t tableId, uint64_t splitKeyHash)
{
    Lock lock(mutex);
    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end())
        throw NoSuchTable(HERE);
    Table* table = it->second;
    Tablet* tablet = findTablet(lock, table, splitKeyHash);
    if (splitKeyHash == tablet->startKeyHash)
        return;
    assert(tablet->status == Tablet::RECOVERING);

    // Perform the split on our in-memory structures.
    table->tablets.push_back(new Tablet(tablet->tableId, splitKeyHash,
            tablet->endKeyHash, tablet->serverId, tablet->status,
            tablet->ctime));
    tablet->endKeyHash = splitKeyHash - 1;

    // No need to record anything in external storage right now. If
    // recovery completes successfully, the Table info will get written
    // to external storage then, including the split information.
    // If the coordinator crashes before completing recovery, it
    // can restart with the old table structure (it will probably just
    // split the tablet again).
    //
    // Also, no need to notify the tablet's current master, since it
    // has crashed.
}

/**
 * Invoked by MasterRecoveryManager after recovery for a tablet has
 * successfully completed to inform coordinator about the new master
 * for the tablet.
 *
 * \param tableId
 *      Id of table containing the tablet.
 * \param startKeyHash
 *      First key hash that is part of range of key hashes for the tablet.
 * \param endKeyHash
 *      Last key hash that is part of range of key hashes for the tablet.
 * \param serverId
 *      Tablet is updated to indicate that it is owned by \a serverId.
 * \param ctime
 *      Tablet is updated with this ctime indicating any object earlier
 *      than \a ctime in its log cannot contain objects belonging to it.
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 */
void
TableManager::tabletRecovered(
        uint64_t tableId, uint64_t startKeyHash, uint64_t endKeyHash,
        ServerId serverId, LogPosition ctime)
{
    Lock lock(mutex);

    // Find the desired table and tablet.
    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end())
        throw NoSuchTablet(HERE);
    Table* table = it->second;
    Tablet* tablet = findTablet(lock, table, startKeyHash);
    if ((tablet->startKeyHash != startKeyHash) ||
            (tablet->endKeyHash != endKeyHash)) {
        throw NoSuchTablet(HERE);
    }

    // Update in-memory data structures.
    tablet->serverId = serverId;
    tablet->status = Tablet::NORMAL;
    tablet->ctime = ctime;

    // Record this update in external storage, in case we crash.  For this
    // operation there is nothing to "complete" after crash recovery other
    // than restoring the table metadata, so the sequence number is set to
    // zero. THIS IS A BUG: see RAM-548.
    ProtoBuf::Table externalInfo;
    serializeTable(lock, table, &externalInfo);
    externalInfo.set_sequence_number(0);
    syncTable(lock, table, &externalInfo);
}

/**
 * Create a table with the given name, if it doesn't already exist.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param name
 *      Name for the table to be created.
 * \param serverSpan
 *      Number of servers across which this table should be split during
 *      creation.
 * \param serverId
 *      Id of the server on which to locate all tablets for this table.
 *
 * \return
 *      Table id of the new table. If a table already exists with the
 *      given name, then its id is returned.
 */
uint64_t
TableManager::createTable(const Lock& lock, const char* name,
        uint32_t serverSpan, ServerId serverId)
{
    // See if the desired table already exists.
    Directory::iterator it = directory.find(name);
    if (it != directory.end())
        return it->second->id;
    uint64_t tableId = nextTableId;

    ++nextTableId;
    LOG(NOTICE, "Creating table '%s' with id %lu", name, tableId);

    if (serverSpan == 0)
        serverSpan = 1;

    // Each iteration through the following loop assigns one tablet
    // for the table to a master.
    Table* table = new Table(name, tableId);
    try {
        uint64_t tabletRange = 1 + ~0UL / serverSpan;
        for (uint32_t i = 0; i < serverSpan; i++) {
            uint64_t startKeyHash = i * tabletRange;
            uint64_t endKeyHash = startKeyHash + tabletRange - 1;
            if (i == (serverSpan - 1))
                endKeyHash = ~0UL;
            ServerId currentTabletMaster;

            if (serverId != ServerId()) {
                currentTabletMaster = serverId;
            } else {
                // Assigned this tablet to the next master in order from the
                // server list.
                currentTabletMaster =
                        context->coordinatorServerList->nextServer(
                                tabletMaster, {WireFormat::MASTER_SERVICE});
                tabletMaster = currentTabletMaster;
            }
            if (!currentTabletMaster.isValid()) {
                // The server list contains no functioning masters! Ask the
                // client to retry, and hope that eventually a master joins
                // the cluster.
                throw RetryException(HERE, 5000000, 10000000,
                        "no masters in cluster");
            }

            // For a new table, set the "creation time" to the beginning of the
            // log. Technically, this is supposed to be the current log head on
            // the master, but retrieving that would require an extra RPC and
            // using (0,0) is safe because we know this is a new table: there
            // can't be any existing information for this table stored on the
            // master.
            LogPosition ctime(0, 0);
            table->tablets.push_back(new Tablet(tableId, startKeyHash,
                    endKeyHash, currentTabletMaster, Tablet::NORMAL, ctime));
        }
    }
    catch (...) {
        delete table;
        throw;
    }
    directory[name] = table;
    idMap[tableId] = table;

    // Create a record in external storage.  If we crash, this will be used
    // by the next coordinator (a) so that it knows about the existence of
    // the table and (b) so it can finish notifying the masters chosen
    // for the tablets, if we crash before we do it.
    ProtoBuf::Table externalInfo;
    serializeTable(lock, table, &externalInfo);
    externalInfo.set_sequence_number(updateManager->nextSequenceNumber());
    externalInfo.set_created(true);
    syncTable(lock, table, &externalInfo);

    // Now notify all the masters about their new table assignments.
    notifyCreate(lock, table);
    updateManager->updateFinished(externalInfo.sequence_number());
    return table->id;
}

/**
 * Delete an index.
 * All existing index data for the index will be deleted, but key values will
 * remain in objects of the table.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param tableId
 *      Id of the table to which the index belongs.
 * \param indexId
 *      Id of the secondary key for which this index stores information.
 * \return
 *      Returns the number of indexlets which exist corresponding to the
 *      dropped index. If index or table doesn't exist, return 0.
 */
void
TableManager::dropIndex(const Lock& lock, uint64_t tableId, uint8_t indexId)
{
    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end()) {
        RAMCLOUD_LOG(NOTICE, "Cannot find table '%lu'", tableId);
        return;
    }
    Table* table = it->second;

    IndexMap::iterator iit = table->indexMap.find(indexId);
    if (iit == table->indexMap.end()) {
        RAMCLOUD_LOG(NOTICE, "Cannot find index '%u' for table '%lu'",
                    indexId, tableId);
        return;
    }

    Index* index = table->indexMap[indexId];
    foreach (Indexlet* indexlet, index->indexlets) {
        backingTableMap.erase(indexlet->backingTableId);
    }

    uint8_t numIndexlets = (uint8_t)index->indexlets.size();

    LOG(NOTICE, "Dropping index '%u' from table '%lu'", indexId, tableId);
    table->indexMap.erase(indexId);
    notifyDropIndex(lock, index);
    delete index;

    for (uint8_t i = 0; i < numIndexlets; i++) {
        string backingTableName;
        backingTableName.append(
            format("__backingTable:%lu:%d:%d", tableId, indexId, i));
        dropTable(lock, backingTableName.c_str());
    }
}

/**
 * Delete the table with the given name. All existing data for the table will
 * be deleted, and the table's name will no longer exist in the directory
 * of tables.
 * 
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param name
 *      Name of the table that is to be dropped.
 */
void
TableManager::dropTable(const Lock& lock, const char* name)
{
    // See if the desired table exists.
    Directory::iterator it = directory.find(name);
    if (it == directory.end())
        return;

    Table* table = it->second;
    LOG(NOTICE, "Dropping table '%s' with id %lu", name, table->id);

    // Record our intention to delete this table.
    ProtoBuf::Table externalInfo;
    serializeTable(lock, table, &externalInfo);
    externalInfo.set_sequence_number(updateManager->nextSequenceNumber());
    externalInfo.set_deleted(true);
    syncTable(lock, table, &externalInfo);

    // Drop all indexes and the backing tables for all indexlets corresponding
    // to each index.
    IndexMap::const_iterator iit = table->indexMap.begin();
    while (iit != table->indexMap.end()) {
        Index* index = iit->second;
        uint8_t numIndexlets = (uint8_t)index->indexlets.size();
        uint8_t indexId = index->indexId;

        LOG(NOTICE, "Dropping index '%u' from table '%lu' ",
                indexId, table->id);
        table->indexMap.erase(iit++);
        notifyDropIndex(lock, index);
        delete index;

        for (uint8_t i = 0; i < numIndexlets; i++) {
            string backingTableName;
            backingTableName.append(
                format("__backingTable:%lu:%d:%d", table->id, indexId, i));
            dropTable(lock, backingTableName.c_str());
        }
    }

    // Delete the table and notify the masters storing its tablets.
    directory.erase(it);
    idMap.erase(table->id);
    delete table;
    notifyDropTable(lock, &externalInfo);
    updateManager->updateFinished(externalInfo.sequence_number());
}

/**
 * Given an index key, find the indexlet containing that entry.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param index
 *      Index in which to search for indexlet.
 * \param key
 *      The secondary index key used to find a particular indexlet.
 * \param keyLength
 *      Length of key.
 *
 * \return
 *      A pointer to the desired indexlet.
 */
TableManager::Indexlet*
TableManager::findIndexlet(const Lock& lock, Index* index,
        const void* key, uint16_t keyLength)
{
    foreach (Indexlet* indexlet, index->indexlets) {
        if ((IndexKey::keyCompare(key, keyLength,
                indexlet->firstKey, indexlet->firstKeyLength) >= 0) &&
            (IndexKey::keyCompare(key, keyLength,
                indexlet->firstNotOwnedKey,
                indexlet->firstNotOwnedKeyLength) < 0)) {
            return indexlet;
        }
    }

    RAMCLOUD_LOG(NOTICE, "Couldn't find specified indexlet in "
            "table id %lu, index id %u", index->tableId, index->indexId);
    return NULL;
}

/**
 * Find the tablet containing a particular key hash.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param table
 *      Table in which to search for tablet.
 * \param keyHash
 *      The desired tablet stores this particular key hash.
 *
 * \return
 *      A pointer to the desired tablet.
 */
Tablet*
TableManager::findTablet(const Lock& lock, Table* table, uint64_t keyHash)
{
    foreach (Tablet* tablet, table->tablets) {
        if ((tablet->startKeyHash <= keyHash) &&
                (tablet->endKeyHash >= keyHash)) {
            return tablet;
        }
    }
    // Shouldn't ever get here:  this means there is some key hash in
    // the table that is not covered by any tablet.
    RAMCLOUD_DIE("Couldn't find tablet containing key hash 0x%lx in "
            "table '%s' (id %lu)",
            keyHash, table->name.c_str(), table->id);
}

/**
 * This method is invoked as part of creating a new table: it sends
 * an RPC to each of the masters storing a tablet for this table, so they
 * know that they are now responsible.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param table
 *      Newly created table; notify the master for each tablet.
 */
void
TableManager::notifyCreate(const Lock& lock, Table* table)
{
    foreach (Tablet* tablet, table->tablets) {
        try {
            LOG(NOTICE, "Assigning table id %lu, key hashes 0x%lx-0x%lx, to "
                    "master %s",
                    table->id, tablet->startKeyHash, tablet->endKeyHash,
                    tablet->serverId.toString().c_str());
            MasterClient::takeTabletOwnership(context, tablet->serverId,
                    tablet->tableId, tablet->startKeyHash, tablet->endKeyHash);
        } catch (ServerNotUpException& e) {
            // The master is apparently crashed. In that case, we can just
            // ignore this master; this tablet will be reinstated elsewhere
            // as part of recovering the master.
            LOG(NOTICE, "takeTabletOwnership skipped for master %s (table %lu, "
                    "key hashes 0x%lx-0x%lx) because server isn't running",
                    tablet->serverId.toString().c_str(), table->id,
                    tablet->startKeyHash, tablet->endKeyHash);
        }
    }
}

/**
 * This method is invoked as part of creating a new index: it sends
 * an RPC to each of the masters storing an indexlet for this index, so they
 * know that they are now responsible.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param index
 *      Newly created index; notify the master for each indexlet.
 */
void
TableManager::notifyCreateIndex(const Lock& lock, Index* index)
{
    foreach (Indexlet* indexlet, index->indexlets) {
        try {
            LOG(NOTICE, "Assigning table id %lu index id %u, "
                        "to master %s", index->tableId, index->indexId,
                        indexlet->serverId.toString().c_str());
            MasterClient::takeIndexletOwnership(context, indexlet->serverId,
                index->tableId, index->indexId, indexlet->backingTableId,
                indexlet->firstKey, indexlet->firstKeyLength,
                indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);
        } catch (ServerNotUpException& e) {
            LOG(NOTICE, "takeIndexletOwnership skipped for master %s "
                    "(table %lu, index %u) because server isn't running",
                    indexlet->serverId.toString().c_str(),
                    index->tableId, index->indexId);
        }
    }
}

/**
 * This method is invoked as part of deleting a table: it sends an RPC
 * to each of the masters storing a tablet for this table, so they
 * can clean up all of their state related to the table. It also performs
 * other cleanup that must be done after the masses have been notified,
 * such as deleting the record for the table in external storage. This
 * method is used both during normal table deletion, and during coordinator
 * crash recovery.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param info
 *      Contains information about all of the tablets in the table.
 */
void
TableManager::notifyDropTable(const Lock& lock, ProtoBuf::Table* info)
{
    // Notify all of the masters storing tablets for the table.
    int numTablets = info->tablet_size();
    for (int i = 0; i < numTablets; i++) {
        const ProtoBuf::Table::Tablet& tablet = info->tablet(i);
        ServerId serverId(tablet.server_id());
        try {
            LOG(NOTICE, "Requesting master %s to drop table id %lu, "
                    "key hashes 0x%lx-0x%lx",
                    serverId.toString().c_str(), info->id(),
                    tablet.start_key_hash(), tablet.end_key_hash());
            MasterClient::dropTabletOwnership(context, serverId,
                    info->id(), tablet.start_key_hash(), tablet.end_key_hash());
        } catch (ServerNotUpException& e) {
            // The master has apparently crashed. This is benign (a dead
            // master can't continue serving the tablet), but log a message
            // anyway.
            LOG(NOTICE, "dropTabletOwnership skipped for master %s (table %lu, "
                    "key hashes 0x%lx-0x%lx) because server isn't running",
                    serverId.toString().c_str(), info->id(),
                    tablet.start_key_hash(), tablet.end_key_hash());
        }
    }

    // If the deleted table's id is the largest one in use, we need
    // to record this on external storage, so the id doesn't get reused
    // if we crash now. Do this *before* removing the external storage record
    // for the table (that record ensures that future coordinators know
    // about its table id).
    if (info->id() == (nextTableId-1)) {
        syncNextTableId(lock);
    }

    // Remove the table's record in external storage.
    string objectName("tables/");
    objectName.append(info->name());
    context->externalStorage->remove(objectName.c_str());
}

/**
 * This method is invoked as part of deleting an index: it sends an RPC
 * to each of the masters storing a indexlet for this index, so they
 * can clean up all of their state related to the index.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param index
 *      Index object corresponding to the index being deleted.
 */
void
TableManager::notifyDropIndex(const Lock& lock, Index* index)
{
    foreach (Indexlet* indexlet, index->indexlets) {
        try {
            LOG(NOTICE, "Requesting master %s to drop table id %lu indexId %u",
                        indexlet->serverId.toString().c_str(), index->tableId,
                        index->indexId);
            MasterClient::dropIndexletOwnership(context, indexlet->serverId,
                index->tableId, index->indexId,
                indexlet->firstKey, indexlet->firstKeyLength,
                indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);
        } catch (ServerNotUpException& e) {

            // The master has apparently crashed. This is benign (a dead
            // master can't continue serving the tablet), but log a message
            // anyway.
            LOG(NOTICE, "dropTabletOwnership skipped for master %s (table %lu, "
                    "index %u) because server isn't running",
                    indexlet->serverId.toString().c_str(), index->tableId,
                    index->indexId);
        }
    }
}

/**
 * This method is invoked as part of reassigning an indexlet: it sends an RPC
 * to the new master to start serving requests for the indexlet.
 * This method is intended to be used both during normal indexlet reassignment,
 * and during coordinator crash recovery.
 * Note: Index related functions are not coordinator crash safe yet,
 * hence this function isn't used during coordinator crash recovery, yet.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param info
 *      Contains information about all of the tablets in the table. Must
 *      contain a "reassign" element.
 */
void
TableManager::notifyReassignIndexlet(const Lock& lock, ProtoBuf::Table* info)
{
    const ProtoBuf::Table::ReassignIndexlet& reassignIndexlet =
        info->reassign_indexlet();
    ServerId serverId(reassignIndexlet.server_id());
    try {
        LOG(NOTICE, "Reassigning an indexlet of index id %u for table id %lu "
                "having backing table id %lu to master %s",
                reassignIndexlet.index_id(), info->id(),
                reassignIndexlet.backing_table_id(),
                serverId.toString().c_str());
        MasterClient::takeIndexletOwnership(context, serverId,
                info->id(), (uint8_t)reassignIndexlet.index_id(),
                reassignIndexlet.backing_table_id(),
                reassignIndexlet.first_key().c_str(),
                (uint16_t)reassignIndexlet.first_key().length(),
                reassignIndexlet.first_not_owned_key().c_str(),
                (uint16_t)reassignIndexlet.first_not_owned_key().length());
    } catch (ServerNotUpException& e) {
        // The master has apparently crashed. This should be benign (we will
        // eventually recover the tablet as part of recovering the master),
        // but log a message anyway.
        LOG(NOTICE, "takeIndexletOwnership failed during indexlet reassignment "
                "for master %s (indexlet of index id %u for table id %lu) "
                "because server isn't running",
                serverId.toString().c_str(),
                reassignIndexlet.index_id(), info->id());
    }
}

/**
 * This method is invoked as part of reassigning a table: it sends an RPC
 * to the new master to start serving requests for the tablet. This
 * method is used both during normal tablet reassignment, and during
 * coordinator crash recovery.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param info
 *      Contains information about all of the tablets in the table. Must
 *      contain a "reassign" element.
 */
void
TableManager::notifyReassignTablet(const Lock& lock, ProtoBuf::Table* info)
{
    const ProtoBuf::Table::Reassign& reassign = info->reassign();
    ServerId serverId(reassign.server_id());
    try {
        LOG(NOTICE, "Reassigning table id %lu, key hashes 0x%lx-0x%lx "
                "to master %s",
                info->id(), reassign.start_key_hash(), reassign.end_key_hash(),
                serverId.toString().c_str());
        MasterClient::takeTabletOwnership(context, serverId, info->id(),
                reassign.start_key_hash(), reassign.end_key_hash());
    } catch (ServerNotUpException& e) {
        // The master has apparently crashed. This should be benign (we will
        // eventually recover the tablet as part of recovering the master),
        // but log a message anyway.
        LOG(NOTICE, "takeTabletOwnership failed during tablet reassignment "
                "for master %s (table %lu, key hashes 0x%lx-0x%lx) because "
                "server isn't running",
                serverId.toString().c_str(), info->id(),
                reassign.start_key_hash(), reassign.end_key_hash());
    }
}

/**
 * This method is invoked as part of splitting a tablet: it sends an RPC
 * to the master storing the tablet being split, so it can update its
 * internal state.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param info
 *      Contains information about the table and the split operation.
 */
void
TableManager::notifySplitTablet(const Lock& lock, ProtoBuf::Table* info)
{
    const ProtoBuf::Table::Split& split = info->split();
    ServerId serverId(split.server_id());
    try {
        LOG(NOTICE, "Requesting master %s to split table id %lu "
                "at key hash 0x%lx",
                serverId.toString().c_str(), info->id(),
                split.split_key_hash());
        MasterClient::splitMasterTablet(context, serverId, info->id(),
                split.split_key_hash());
    } catch (ServerNotUpException& e) {
        // The master has apparently crashed. This is benign (crash recovery
        // will take care of splitting the tablet), but log a message
        // anyway.
        LOG(NOTICE, "splitMasterTablet skipped for master %s (table %lu, "
                "split key hash 0x%lx) because server isn't running",
                serverId.toString().c_str(), info->id(),
                split.split_key_hash());
    }
}

/**
 * This method re-creates the internal data structures for a table, based
 * on a protocol buffer read from external storage.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param info
 *      Describes one table.
 */
TableManager::Table*
TableManager::recreateTable(const Lock& lock, ProtoBuf::Table* info)
{
    const string& name(info->name());
    uint64_t id = info->id();
    Directory::iterator it = directory.find(name);
    if (it != directory.end()) {
        throw FatalError(HERE, format("can't recover table '%s' (id %lu): "
                "already exists in directory",
                name.c_str(), id));
    }
    if (idMap.count(id) != 0) {
        throw FatalError(HERE,
                format("can't recover table '%s' (id %lu): already "
                "exists in idMap",
                name.c_str(), id));
    }

    Table* table = new Table(name.c_str(), id);
    int numTablets = info->tablet_size();
    for (int i = 0; i < numTablets; i++) {
        const ProtoBuf::Table::Tablet& tabletInfo = info->tablet(i);
        LogPosition ctime(tabletInfo.ctime_log_head_id(),
                tabletInfo.ctime_log_head_offset());
        Tablet::Status status;
        if (tabletInfo.state() == ProtoBuf::Table::Tablet::NORMAL)
            status = Tablet::NORMAL;
        else if (tabletInfo.state() == ProtoBuf::Table::Tablet::RECOVERING)
            status = Tablet::RECOVERING;
        else
            DIE("Unknown status for tablet");
        Tablet* tablet = new Tablet(id,
                tabletInfo.start_key_hash(),
                tabletInfo.end_key_hash(),
                ServerId(tabletInfo.server_id()),
                status,
                LogPosition(tabletInfo.ctime_log_head_id(),
                              tabletInfo.ctime_log_head_offset()));
        table->tablets.push_back(tablet);
        LOG(NOTICE, "Recovered tablet 0x%lx-0x%lx for table '%s' (id %lu) "
                "on server %s", tablet->startKeyHash, tablet->endKeyHash,
                name.c_str(), tablet->tableId,
                tablet->serverId.toString().c_str());
    }
    directory[name] = table;
    idMap[id] = table;
    return table;
}

/**
 * This method is used when recording information on external storage;
 * it initializes a protocol buffer with the current state of a table.
 * Typically, the caller will then put additional information in the
 * protocol buffer describing operations in progress, for recovery
 * purposes.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param table
 *      Table whose information should be serialized into the protocol buffer.
 * \param[out] externalInfo
 *      Information gets serialized here; we assume that this is a
 *      clean, freshly-allocated object.
 */
void
TableManager::serializeTable(const Lock& lock, Table* table,
        ProtoBuf::Table* externalInfo)
{
    externalInfo->set_name(table->name);
    externalInfo->set_id(table->id);
    foreach (Tablet* tablet, table->tablets) {
        ProtoBuf::Table::Tablet* externalTablet(externalInfo->add_tablet());
        externalTablet->set_start_key_hash(tablet->startKeyHash);
        externalTablet->set_end_key_hash(tablet->endKeyHash);
        if (tablet->status == Tablet::NORMAL)
            externalTablet->set_state(ProtoBuf::Table::Tablet::NORMAL);
        else if (tablet->status == Tablet::RECOVERING)
            externalTablet->set_state(ProtoBuf::Table::Tablet::RECOVERING);
        else
            DIE("Unknown status for tablet");
        externalTablet->set_server_id(tablet->serverId.getId());
        externalTablet->set_ctime_log_head_id(tablet->ctime.getSegmentId());
        externalTablet->set_ctime_log_head_offset(
                tablet->ctime.getSegmentOffset());
    }
}

/**
 * Update next_table_id on external storage.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 */
void
TableManager::syncNextTableId(const Lock& lock)
{
    ProtoBuf::TableManager info;
    info.set_next_table_id(nextTableId);
    string str;
    info.SerializeToString(&str);
    context->externalStorage->set(ExternalStorage::UPDATE,
            "tableManager", str.c_str(), downCast<int>(str.length()));
}

/**
 * Write the latest information about a table to the appropriate place in
 * external storage.
 *
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \param table
 *      Table whose description is in externalInfo.
 * \param externalInfo
 *      Information about the table that we want to save to external storage;
 *      caller has filled this in.
 */
void
TableManager::syncTable(const Lock& lock, Table* table,
        ProtoBuf::Table* externalInfo)
{
    string objectName("tables/");
    objectName.append(table->name);
    string str;
    externalInfo->SerializeToString(&str);
    context->externalStorage->set(ExternalStorage::UPDATE,
            objectName.c_str(), str.c_str(),
            downCast<int>(str.length()));
}

/**
 * Add a new tablet to the information stored for particular table.
 * This method is intended only for testing and is not safe to use
 * iin any "real" context.
 *
 * \param tablet
 *      Describes the new tablet to add to the TableManager data structures.
 *      The associated table must already exist.
 */
void
TableManager::testAddTablet(const Tablet& tablet)
{
    Lock lock(mutex);
    IdMap::iterator it = idMap.find(tablet.tableId);
    if (it == idMap.end())
        throw FatalError(HERE, "table doesn't exist");
    Table* table = it->second;
    table->tablets.push_back(new Tablet(tablet));
}

/**
 * Create a new table with no tablets. This method is intended only for
 * testing and is not safe to use in any "real" context.
 *
 * \param name
 *      Textual name for the new table. This name must not already be
 *      in use.
 * \param id
 *      Identifier for the new table. This identifier must not already
 *      be in use.
 */
void
TableManager::testCreateTable(const char* name, uint64_t id)
{
    Lock lock(mutex);
    Table* table = new Table(name, id);
    directory[name] = table;
    idMap[id] = table;
    if (nextTableId <= id)
        nextTableId = id+1;
}

/**
 * Find the tablet containing a particular keyHash for a given tableId. This
 * method is used only in unit tests to check that a tablet exits and has the
 * correct information.
 *
 * \param tableId
 *      Id of table in which to search for tablet.
 * \param keyHash
 *      The desired tablet stores this particular key hash.
 *
 * \return
 *      A pointer to the desired tablet.  NULL if no matching tablet found.
 */
Tablet*
TableManager::testFindTablet(uint64_t tableId, uint64_t keyHash)
{
    Lock lock(mutex);
    IdMap::iterator it = idMap.find(tableId);
    if (it == idMap.end())
        return NULL;
    Table* table = it->second;
    foreach (Tablet* tablet, table->tablets) {
        if ((tablet->startKeyHash <= keyHash) &&
                (tablet->endKeyHash >= keyHash)) {
            return tablet;
        }
    }
    return NULL;
}

} // namespace RAMCloud
