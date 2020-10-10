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
#include <algorithm>

#include "ClientException.h"
#include "TabletManager.h"
#include "TimeTrace.h"
#include "Util.h"

namespace RAMCloud {

TabletManager::TabletManager()
    : tabletMap()
    , lock("TabletManager::lock")
    , numLoadingTablets(0)
{
}

/**
 * Add a new tablet to this TabletManager's list of tablets. If the tablet
 * already exists or overlaps with any other tablets, the call will fail.
 *
 * \param tableId
 *      Identifier of the table the new tablet belongs to.
 * \param startKeyHash
 *      The first key hash value that this tablet owns.
 * \param endKeyHash
 *      The last key hash value that this tablet owns.
 * \param state
 *      The initial state of the tablet (see the TabletState enum for more
 *      details).
 * \return
 *      Returns true if successfully added, false if the tablet cannot be
 *      added because it overlaps with one or more existing tablets.
 */
bool
TabletManager::addTablet(uint64_t tableId,
                         uint64_t startKeyHash,
                         uint64_t endKeyHash,
                         TabletState state)
{
    SpinLock::Guard guard(lock);

    // If an existing tablet overlaps this range at all, fail.
    if (lookup(tableId, startKeyHash, guard) != tabletMap.end() ||
      lookup(tableId, endKeyHash, guard) != tabletMap.end()) {
        return false;
    }

    tabletMap.insert(std::make_pair(tableId,
                     Tablet(tableId, startKeyHash, endKeyHash, state)));

    if (state == TabletState::NOT_READY) {
        numLoadingTablets++;
    }

    return true;
}

/**
 * Given a key, determine whether a tablet exists for this key and has status
 * NORMAL.  We simultaneously increments the read count on the tablet. This is
 * called by ObjectManger::readObject to avoid looking up the Tablet twice, for
 * verification of state and incrementing the read count.
 *
 * \param key
 *      The Key whose tablet we're looking up.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::checkAndIncrementReadCount(Key& key) {
    SpinLock::Guard guard(lock);
    TabletMap::iterator it = lookup(key.getTableId(), key.getHash(), guard);

    if (it == tabletMap.end())
        return false;
    if (it->second.state != NORMAL) {
        if (it->second.state == TabletManager::LOCKED_FOR_MIGRATION)
            throw RetryException(HERE, 1000, 2000,
                    "Tablet is currently locked for migration!");
        return false;
    }

    it->second.readCount++;
    return true;
}

/**
 * Given a key, obtain the data of the tablet associated with that key, if one
 * exists. Note that the data returned is a snapshot. The TabletManager's data
 * may be modified at any time by other threads.
 *
 * \param key
 *      The Key whose tablet we're looking up.
 * \param outTablet
 *      Optional pointer to a Tablet object that will be filled with the current
 *      appopriate tablet data, if such a tablet exists. May be NULL if the
 *      caller only wants to check for existence.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::getTablet(Key& key, Tablet* outTablet)
{
    return getTablet(key.getTableId(), key.getHash(), outTablet);
}

/**
 * Given a tableId and hash value, obtain the data of the tablet associated
 * with them, if one exists. Note that the data returned is a snapshot. The
 * TabletManager's data may be modified at any time by other threads.
 *
 * \param tableId
 *      The table identifier of the tablet we're looking up.
 * \param keyHash
 *      Key hash value whose tablet we're looking up.
 * \param outTablet
 *      Optional pointer to a Tablet object that will be filled with the current
 *      appopriate tablet data, if such a tablet exists. May be NULL if the caller
 *      only wants to check for existence.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::getTablet(uint64_t tableId, uint64_t keyHash, Tablet* outTablet)
{
    SpinLock::Guard guard(lock);

    TabletMap::iterator it = lookup(tableId, keyHash, guard);
    if (it == tabletMap.end())
        return false;

    if (outTablet != NULL)
        *outTablet = it->second;
    return true;
}

/**
 * Given the exact specification of a tablet's range (table identifier and start
 * and end hash values), obtain the current data associated with that tablet, if
 * it exists. Note that the data returned is a snapshot. The TabletManager's data
 * may be modified at any time by other threads.
 *
 * \param tableId
 *      The table identifier of the tablet we're looking up.
 * \param startKeyHash
 *      Key hash value at which the desired tablet begins.
 * \param endKeyHash
 *      Key hash value at which the desired tablet ends.
 * \param outTablet
 *      Optional pointer to a Tablet object that will be filled with the current
 *      appopriate tablet data, if such a tablet exists. May be NULL if the caller
 *      only wants to check for existence.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::getTablet(uint64_t tableId,
                         uint64_t startKeyHash,
                         uint64_t endKeyHash,
                         Tablet* outTablet)
{
    SpinLock::Guard guard(lock);

    TabletMap::iterator it = lookup(tableId, startKeyHash, guard);
    if (it == tabletMap.end())
        return false;

    Tablet* t = &it->second;
    if (t->startKeyHash != startKeyHash || t->endKeyHash != endKeyHash)
        return false;

    if (outTablet != NULL)
        *outTablet = *t;
    return true;
}

/**
 * Fill in the given vector with data from all of the tablets this TabletManager
 * is currently keeping track of. The results are unsorted.
 *
 * \param outTablets
 *      Pointer to the vector to append tablet data to.
 */
void
TabletManager::getTablets(vector<Tablet>* outTablets)
{
    SpinLock::Guard _(lock);

    TabletMap::iterator it = tabletMap.begin();
    for (size_t i = 0; it != tabletMap.end(); i++) {
        outTablets->push_back(it->second);
        ++it;
    }
}

/**
 * Remove a tablet previously created by addTablet() or splitTablet() and delete
 * all data that tracks its existence.
 * If such a tablet does not exist, ignore and return successfully.
 *
 * \param tableId
 *      The table identifier of the tablet we're deleting.
 * \param startKeyHash
 *      Key hash value at which the to-be-deleted tablet begins.
 * \param endKeyHash
 *      Key hash value at which the to-be-deleted tablet ends.
 * \return
 *      True if a tablet was removed; false otherwise.
 * \throw
 *      InternalError if tablet was not found because the range overlaps
 *      with one or more existing tablets.
 */
bool
TabletManager::deleteTablet(uint64_t tableId,
                            uint64_t startKeyHash,
                            uint64_t endKeyHash)
{
    SpinLock::Guard guard(lock);

    TabletMap::iterator it = lookup(tableId, startKeyHash, guard);
    if (it == tabletMap.end()) {
        RAMCLOUD_LOG(DEBUG, "Could not find tablet in tableId %lu with "
                            "startKeyHash %lu and endKeyHash %lu",
                            tableId, startKeyHash, endKeyHash);
        return false;
    }

    Tablet* t = &it->second;
    if (t->startKeyHash != startKeyHash || t->endKeyHash != endKeyHash) {
        RAMCLOUD_LOG(ERROR, "Could not find tablet in tableId %lu with "
                            "startKeyHash %lu and endKeyHash %lu: "
                            "overlaps with one or more other ranges",
                            tableId, startKeyHash, endKeyHash);
        throw InternalError(HERE, STATUS_INTERNAL_ERROR);
    }

    tabletMap.erase(it);

    if (t->state == TabletState::NOT_READY) {
        numLoadingTablets--;
    }

    return true;
}

/**
 * SplitTableLone
 * Utilitary to split a tablet and create a tablet containing the target object to migrate 
 * alone. To reduce the cost for migrates or recover 
 */
bool
TabletManager::splitTabletLone(uint64_t tableId,
                           uint64_t splitKeyHash)
{
    SpinLock::Guard guard(lock);

    TabletMap::iterator it = lookup(tableId, splitKeyHash, guard);
    if (it == tabletMap.end())
        return false;

    Tablet* t = &it->second;

    // If a split already exists in the master's tablet map, lookup
    // will return the tablet whose startKeyHash matches splitKeyHash.
    // So to make it idempotent, check for this condition before you
    // decide to do the split
    if (splitKeyHash != t->startKeyHash) {
        tabletMap.insert(std::make_pair(tableId, Tablet
                         (tableId, splitKeyHash, splitKeyHash, t->state)));
        tabletMap.insert(std::make_pair(tableId, Tablet 
                          (tableId,splitKeyHash+1,t->endKeyHash,t->state)));
        
        t->endKeyHash = splitKeyHash - 1;

        // It's unclear what to do with the counts when splitting. The old
        // behavior was to simply zero them, so for the time being we'll
        // stick with that. At the very least it's what Christian expects.
        t->readCount = t->writeCount = 0;

        if (t->state == TabletState::NOT_READY) {
            numLoadingTablets++;
        }
    }

    return true;
}

/**
 * Split an existing tablet into two new, contiguous tablets. This may be used
 * prior to migrating objects to another server if only a portion of a tablet
 * needs to be moved.
 *
 * \param tableId
 *      Table identifier corresponding to the tablet to be split.
 * \param splitKeyHash
 *      The point at which to split the tablet. This value must be strictly
 *      between (but not equal to) startKeyHash and endKeyHash. The tablet
 *      will be split into two pieces: [startKeyHash, splitKeyHash - 1] and
 *      [splitKeyHash, endKeyHash].
 * \return
 *      True if the tablet was found and split or if the split already exists.
 *      False if no tablet corresponding to the given (tableId, splitKeyHash)
 *      tuple was found. This operation is idempotent.
 */
bool
TabletManager::splitTablet(uint64_t tableId,
                           uint64_t splitKeyHash)
{
    SpinLock::Guard guard(lock);

    TabletMap::iterator it = lookup(tableId, splitKeyHash, guard);
    if (it == tabletMap.end())
        return false;

    Tablet* t = &it->second;

    // If a split already exists in the master's tablet map, lookup
    // will return the tablet whose startKeyHash matches splitKeyHash.
    // So to make it idempotent, check for this condition before you
    // decide to do the split
    if (splitKeyHash != t->startKeyHash) {
        tabletMap.insert(std::make_pair(tableId, Tablet
                         (tableId, splitKeyHash, t->endKeyHash, t->state)));
        t->endKeyHash = splitKeyHash - 1;

        // It's unclear what to do with the counts when splitting. The old
        // behavior was to simply zero them, so for the time being we'll
        // stick with that. At the very least it's what Christian expects.
        t->readCount = t->writeCount = 0;

        if (t->state == TabletState::NOT_READY) {
            numLoadingTablets++;
        }
    }

    return true;
}

/**
 * Transition the state field associated with a given tablet from a specific
 * old state to a given new state. This is typically used when recovery has
 * completed and a tablet is changed from the NOT_READY to NORMAL state.
 *
 * \param tableId
 *      Table identifier corresponding to the tablet to update.
 * \param startKeyHash
 *      First key hash value corresponding to the tablet to update.
 * \param endKeyHash
 *      Last key hash value corresponding to the tablet to update.
 * \param oldState
 *      The state the tablet is expected to be in prior to changing to the new
 *      value. This helps ensure that the proper transition is made, since
 *      another thread could modify the tablet's state between getTablet() and
 *      changeState() calls.
 * \param newState
 *      The state to transition the tablet to.
 * \return
 *      Returns true if the state was updated, otherwise false.
 */
bool
TabletManager::changeState(uint64_t tableId,
                           uint64_t startKeyHash,
                           uint64_t endKeyHash,
                           TabletState oldState,
                           TabletState newState)
{
    SpinLock::Guard guard(lock);

    TabletMap::iterator it = lookup(tableId, startKeyHash, guard);
    if (it == tabletMap.end())
        return false;

    Tablet* t = &it->second;
    if (t->startKeyHash != startKeyHash || t->endKeyHash != endKeyHash)
        return false;

    if (t->state != oldState)
        return false;

    t->state = newState;

    assert(oldState != newState);
    if (newState == TabletState::NOT_READY) {
        numLoadingTablets++;
    } else if (oldState == TabletState::NOT_READY) {
        numLoadingTablets--;
    }

    return true;
}

/**
 * Increment the object read counter on the tablet associated with the given
 * key.
 */
void
TabletManager::incrementReadCount(Key& key)
{
    incrementReadCount(key.getTableId(), key.getHash());
}

/**
 * Increment the object read counter on the tablet associated with the given
 * table id and primary key hash.
 */
void
TabletManager::incrementReadCount(uint64_t tableId, KeyHash keyHash)
{
    SpinLock::Guard guard(lock);
    TabletMap::iterator it = lookup(tableId, keyHash, guard);
    if (it != tabletMap.end())
        it->second.readCount++;
}

/**
 * Increment the object write counter on the tablet associated with the given
 * key.
 */
void
TabletManager::incrementWriteCount(Key& key)
{
    incrementWriteCount(key.getTableId(), key.getHash());
}

/**
 * Increment the object write counter on the tablet associated with the given
 * table id and primary key hash.
 */
void
TabletManager::incrementWriteCount(uint64_t tableId, KeyHash keyHash)
{
    SpinLock::Guard guard(lock);
    TabletMap::iterator it = lookup(tableId, keyHash, guard);
    if (it != tabletMap.end())
        it->second.writeCount++;
}

/**
 * Populate a ServerStatistics protocol buffer with read and write statistics
 * gathered for our tablets.
 */
void
TabletManager::getStatistics(ProtoBuf::ServerStatistics* serverStatistics)
{
    SpinLock::Guard _(lock);

    TabletMap::iterator it = tabletMap.begin();
    while (it != tabletMap.end()) {
        Tablet* t = &it->second;
        ProtoBuf::ServerStatistics_TabletEntry* entry =
            serverStatistics->add_tabletentry();
        entry->set_table_id(t->tableId);
        entry->set_start_key_hash(t->startKeyHash);
        entry->set_end_key_hash(t->endKeyHash);
        uint64_t totalOperations = t->readCount + t->writeCount;
        if (totalOperations > 0)
            entry->set_number_read_and_writes(totalOperations);
        ++it;
    }
}

/**
 * Obtain the total number of tablets this object is managing.
 */
size_t
TabletManager::getNumTablets()
{
    SpinLock::Guard _(lock);
    return tabletMap.size();
}


/**
 * Helper function to toString(); used to print a single tablet to a String.
 *
 * \param tablet - tablet to print out
 * \param output - string to append output to
 */
static void
printTablet(TabletManager::Tablet *tablet, string* output) {
    if (output->length() != 0)
        output->append("\n");
    *output += format("{ tableId: %lu startKeyHash: %lu endKeyHash: %lu "
        "state: %d reads: %lu writes: %lu }", tablet->tableId,
        tablet->startKeyHash, tablet->endKeyHash, tablet->state,
        tablet->readCount, tablet->writeCount);
}

/**
 * Helper function to toString(); used to std::sort() tablets first by their
 * tableId and then by their start hash.
 *
 * \param a - tablet 1
 * \param b - tablet 2
 * \return  - true if a < b
 */
bool
compareTablet(const TabletManager::Tablet &a,
              const TabletManager::Tablet &b) {
    if (a.tableId != b.tableId)
        return a.tableId < b.tableId;
    else
        return a.startKeyHash < b.startKeyHash;
}

/**
 * Obtain a string representation of the tablets this object is managing.
 * Tablets are unordered in the output. This is typically used in unit tests.
 */
string
TabletManager::toString()
{
    SpinLock::Guard _(lock);
    string output;

// Sort output on debug
#if DEBUG_BUILD
    TabletMap::iterator it;
    vector<Tablet> tablets;

    for (it = tabletMap.begin(); it != tabletMap.end(); ++it)
        tablets.push_back(it->second);
    sort(tablets.begin(), tablets.end(), compareTablet);

    for (size_t i = 0; i < tablets.size(); ++i)
        printTablet(&tablets[i], &output);
#else
    for (TabletMap::iterator it = tabletMap.begin();
            it != tabletMap.end(); ++it) {
       printTablet(&(it->second), &output);
    }
#endif

    return output;
}

/**
 * Helper for the public methods that need to look up a tablet. This method
 * iterates over all candidates in the multimap.
 *
 * \param tableId
 *      Identifier of the table to look up.
 * \param keyHash
 *      Key hash value corresponding to the desired tablet.
 * \param lock
 *      Ensures that the caller holds the monitor lock; not actually used.
 * \return
 *      A TabletMap::iterator is returned. If no tablet was found, it will be
 *      equal to tabletMap.end(). Otherwise, it will refer to the desired
 *      tablet.
 *
 *      An iterator, rather than a Tablet pointer is returned to facilitate
 *      efficient deletion.
 */
TabletManager::TabletMap::iterator
TabletManager::lookup(uint64_t tableId, uint64_t keyHash,
                      const SpinLock::Guard& lock)
{
    auto range = tabletMap.equal_range(tableId);
    TabletMap::iterator end = range.second;
    for (TabletMap::iterator it = range.first; it != end; it++) {
        Tablet* t = &it->second;
        if (keyHash >= t->startKeyHash && keyHash <= t->endKeyHash)
            return it;
    }

    return tabletMap.end();
}

/**
 * Construct to freeze the state of tabletManager from outside.
 *
 * \param tabletManager
 *      The TabletManager instances that shouldn't be modified.
 */
TabletManager::Protector::Protector(TabletManager* tabletManager)
    : tabletManager(tabletManager),
      lockGuardForTabletManager(tabletManager->lock)
{
}

/**
 * Tells whether any tablet in this master is in NOT_READY status.
 *
 * \return
 *      true if there is a tablet with NOT_READY status.
 */
bool
TabletManager::Protector::notReadyTabletExists()
{
    return tabletManager->numLoadingTablets > 0;
}

/**
 * Given a tableId and hash value, obtain the data of the tablet associated
 * with them, if one exists. Note that the data returned is a snapshot. The
 * TabletManager's data will not be modified until at any time by other threads.
 *
 * \param tableId
 *      The table identifier of the tablet we're looking up.
 * \param keyHash
 *      Key hash value whose tablet we're looking up.
 * \param outTablet
 *      Optional pointer to a Tablet object that will be filled with the current
 *      appopriate tablet data, if such a tablet exists. May be NULL if the caller
 *      only wants to check for existence.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::Protector::getTablet(uint64_t tableId, uint64_t keyHash,
                                    Tablet* outTablet)
{
    TabletMap::iterator it = tabletManager->lookup(tableId, keyHash,
                                                   lockGuardForTabletManager);
    if (it == tabletManager->tabletMap.end())
        return false;

    if (outTablet != NULL)
        *outTablet = it->second;
    return true;
}

} // namespace
