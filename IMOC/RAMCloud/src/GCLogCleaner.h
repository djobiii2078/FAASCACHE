/* Copyright (c) 2010-2015 Stanford University
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

#include <assert.h>
#include <stdint.h>
#include <sys/sysinfo.h>


#include "Common.h"
#include "Fence.h"
#include "Log.h"
#include "GCLogCleaner.h"
#include "PerfStats.h"
#include "ShortMacros.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "ServerConfig.h"
#include "WallTime.h"
#include "ObjectManager.h"

namespace RAMCloud {

/**
 * Construct a new LogCleaner object. The cleaner will not perform any garbage
 * collection until the start() method is invoked.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param config
 *      Server configuration from which the cleaner will extract any runtime
 *      parameters that affect its operation.
 * \param segmentManager
 *      The SegmentManager to query for newly cleanable segments, allocate
 *      survivor segments from, and report cleaned segments to.
 * \param replicaManager
 *      The ReplicaManager to use in backing up segments written out by the
 *      cleaner.
 * \param entryHandlers
 *     Class responsible for entries stored in the log. The cleaner will invoke
 *     it when they are being relocated during cleaning, for example.
 */
GCLogCleaner::GCLogCleaner(Context* context,
                       const ServerConfig* config,
                       SegmentManager& segmentManager,
                       ReplicaManager& replicaManager,
                       LogEntryHandlers& entryHandlers,
                       ObjectManager* objectManager)
    : context(context),
      segmentManager(segmentManager),
      replicaManager(replicaManager),
      entryHandlers(entryHandlers),
      objectManager(objectManager),
      cleanableSegments(segmentManager, config, context, onDiskMetrics),
      writeCostThreshold(config->master.cleanerWriteCostThreshold),
      disableInMemoryCleaning(config->master.disableInMemoryCleaning),
      numThreads(config->master.cleanerThreadCount),
      segletSize(config->segletSize),
      segmentSize(config->segmentSize),
      activeGCThreads(0),
      disableCount(0),
      disableGCCount(0),
      cleanerIdle(),
      mutex(),
      doWorkTicks(0),
      doGCTicks(0),
      doGCSortTicks(0),
      doWorkSleepTicks(0),
      doEvictTicks(0),
      inMemoryMetrics(),
      onDiskMetrics(),
      threadMetrics(numThreads),
      threadsShouldExit(false),
      threads(),
      GCthreads(),
      balancer(NULL)
{
    
    balancer = new FixedBalancer(this, 0);
    
    for (int i = 0; i < numThreads; i++){
        GCthreads.push_back(NULL);
    }
        
}

/**
 * Destroy the cleaner. Any running threads are stopped first.
 */
GCLogCleaner::~GCLogCleaner()
{
    stop();
    delete balancer;
    TEST_LOG("destroyed GCLogCleaner");
}

/**
 * Start the log cleaner, if it isn't already running. This spins a thread that
 * continually cleans if there's work to do until stop() is called.
 *
 * The cleaner will not do any work until explicitly enabled via this method.
 *
 * This method may be called any number of times, but it is not thread-safe.
 * That is, do not call start() and stop() in parallel.
 */
void
GCLogCleaner::start()
{
    for (int i = 0; i < numThreads; i++) {
        if (GCthreads[i] == NULL)
            GCthreads[i] = new std::thread(GCleanerThreadEntry, this, context);
    }
}

/**
 * Halt the cleaner thread (if it is running). Once halted, it will do no more
 * work until start() is called again.
 *
 * This method may be called any number of times, but it is not thread-safe.
 * That is, do not call start() and stop() in parallel.
 */
void
GCLogCleaner::stop()
{
    threadsShouldExit = true;
    Fence::sfence();

    for (int i = 0; i < numThreads; i++) {

        if (GCthreads[i] != NULL) {
            GCthreads[i]->join();
            delete GCthreads[i];
            GCthreads[i] = NULL; 
        }
        
    }

    threadsShouldExit = false;

    /// XXX- should set threadCnt to 0 so we can clean on disk again!
}

/**
 * Fill in the provided protocol buffer with metrics, giving other modules and
 * servers insight into what's happening in the cleaner.
 */
void
GCLogCleaner::getMetrics(ProtoBuf::LogMetrics_CleanerMetrics& m)
{
    LOG(NOTICE, "NOT IMPLEMENTED");
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

static volatile uint32_t GCthreadCnt;

/**
 * Static entry point for the GCleaner thread. This is invoked via the
 * std::thread() constructor. This thread performs continuous cleaning on an
 * as-needed basis.
 */


void 
GCLogCleaner::GCleanerThreadEntry(LogCleaner* logCleaner, Context* context)
{
    LOG(NOTICE, "Garbage Collector thread started");

    GCThreadState state; 
    state.threadNumber = __sync_fetch_and_add(&GCthreadCnt,1);
    try {
        while (1) {
            Fence::lfence();
            if(GClogCleaner->threadsShouldExit)
                break;
            GClogCleaner->doGCCleaning(&state);
        }
    }catch (const Exception& e) {
        DIE("Fatal error in GC thread: %s",e.what());
    }

    LOG(NOTICE, "LogCleaner GC thread stopping");
}



//Returns the number of bytes cleaned by the GC
int GCLogCleaner::gClean()
{
    //Get objects that have a count less than 
    //Garbage Collector Threshold 
    Lock lock(mutex);

    //Update the list of unused objects.
    //Let's sort the list of entries 
    
    {
        AtomicCycleCounter _(&doGCSortTicks);
        std::sort(objectTracked.begin(),objectTracked.end(), AccessTimeComparer());
    }

    std::unordered_map<uint64_t, ObjectTracker>::iterator it = objectTracked.begin();

    if(it == objectTracked.end())
    {
        LOG(NOTICE, "[RC_FAAS] No object to clean with Garbage Collector");
        return 0;
    }

    LogEntryType type;
    Buffer buffer;
    Log::Reference reference;
    while(it != objectTracked.end())
    {
        if(it->second.readCount < GARABAGE_COLLECTOR_THRESHOLD)
        {
            //We need to slash this one out 
            lookup(lock, key, type, buffer, NULL, &reference);
        }

        it++;
    }
        

    //This is called to remove all the tombstones created by deleting 
    //objects of the garbage collector. 
    //Log cleaned objects and size 
    doMemoryCleaning();
    return ; 
}


/**
 * Main loop for garbage collector
 */

void 
LogCleaner::doGCCleaning(GCThreadState* state)
{
    AtomicCycleCounter _(&doGCTicks);

    bool goToSleep = false;
    {
        Lock lock(mutex);
        activeGCThreads++;
        if(disableGCCount) {
            goToSleep = true; 
        }
    }

    if(!goToSleep){

        CycleCounter<uint64_t> __(&state->gCleanTicks);
        gClean();
        switch (balancer->needsGC(state)){
            
            case Balancer::EVICT_MEM:
            {
                /* This means, we are in need of memory */
                /* We can start migrating objects from one node to another if any candidate node present
                /* otherwise just discard the objects */

                //Obtain amount of memory to discard
                //This value is obtained by getting the physical amount of memory * percentage_to_discard.
                uint64_t memToFree = (EVICT_PERCENTAGE * sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE))/100;

                //Now we can in a round robin fashion migrate per block of 128 MB
                //Tablets to other nodes then free the memory held by the segment manager 
                //migrateObjectAndFree()
                break;
            }

            case Balancer::INCREASE_MEM:
            {
                //Launch here the script to squeeze out memory from running containers
                break; 
            }
        }
    }

     {
        // Wake up disablers, if any are waiting for the cleaner to go idle.
        Lock lock(mutex);
        activeGCThreads--;
        if ((activeGCThreads == 0) && (disableGCCount != 0)) {
            cleanerIdle.notify_all(); //Needs to update to notify_all_GC()
        }
    }

    if (goToSleep) {
        AtomicCycleCounter __(&doSleepGCTicks);
        // Jitter the sleep delay a little bit (up to 10%). It's not a big deal
        // if we don't, but it can make some locks look artificially contended
        // when there's no cleaning to be done and threads manage to caravan
        // together.
        useconds_t r = downCast<useconds_t>(generateRandom() % POLL_GC_USEC) / 10;
        usleep(POLL_GC_USEC + r);
    }
}

/**
 * Main cleaning loop, constantly invoked via cleanerThreadEntry(). If there
 * is cleaning to be done, do it now return. If no work is to be done, sleep for
 * a bit before returning (and getting called again), rather than banging on the
 * CPU.
 */
void
LogCleaner::doWork(CleanerThreadState* state)
{
    AtomicCycleCounter _(&doWorkTicks);


    bool goToSleep = false;
    {
        // See if we have been disabled.
        Lock lock(mutex);
        activeThreads++;
        if (disableCount) {
            goToSleep = true;
        }
    }
    if (!goToSleep) {
        threadMetrics.noteThreadStart();
        switch (balancer->requestTask(state)) {
        case Balancer::CLEAN_DISK:
          {
            CycleCounter<uint64_t> __(&state->diskCleaningTicks);
            doDiskCleaning();
            break;
          }

        case Balancer::COMPACT_MEMORY:
          {
            CycleCounter<uint64_t> __(&state->memoryCompactionTicks);
            doMemoryCleaning();
            break;
          }

        case Balancer::SLEEP:
            goToSleep = true;
            break;
        }

        threadMetrics.noteThreadStop();
    }

    {
        // Wake up disablers, if any are waiting for the cleaner to go idle.
        Lock lock(mutex);
        activeThreads--;
        if ((activeThreads == 0) && (disableCount != 0)) {
            cleanerIdle.notify_all();
        }
    }

    if (goToSleep) {
        AtomicCycleCounter __(&doWorkSleepTicks);
        // Jitter the sleep delay a little bit (up to 10%). It's not a big deal
        // if we don't, but it can make some locks look artificially contended
        // when there's no cleaning to be done and threads manage to caravan
        // together.
        useconds_t r = downCast<useconds_t>(generateRandom() % POLL_USEC) / 10;
        usleep(POLL_USEC + r);
    }
}

/**
 * Perform an in-memory cleaning pass. This takes a segment and compacts it,
 * re-packing all live entries together sequentially, allowing us to reclaim
 * some of the dead space.
 */
void
LogCleaner::doMemoryCleaning()
{
    TEST_LOG("called");
    AtomicCycleCounter _(&inMemoryMetrics.totalTicks);
    uint64_t startTicks = Cycles::rdtsc();

    if (disableInMemoryCleaning)
        return;

    LogSegment* segment = getSegmentToCompact();
    if (segment == NULL)
        return;

    LogCleanerMetrics::InMemory<uint64_t> localMetrics;

    // If the segment happens to be empty then there's no data to move.
    const bool empty = (segment->getLiveBytes() == 0);
    if (empty)
        localMetrics.totalEmptySegmentsCompacted++;

    // Allocate a survivor segment to write into. This call may block if one
    // is not available right now.
    CycleCounter<uint64_t> waitTicks(&localMetrics.waitForFreeSurvivorTicks);
    LogSegment* survivor = segmentManager.allocSideSegment(
            SegmentManager::FOR_CLEANING | SegmentManager::MUST_NOT_FAIL,
            segment);
    assert(survivor != NULL);
    waitTicks.stop();

    localMetrics.totalBytesInCompactedSegments +=
        segment->getSegletsAllocated() * segletSize;
    uint32_t liveScannedEntryTotalLengths[TOTAL_LOG_ENTRY_TYPES] = { 0 };

    // Take two passes, writing out the tombstones first. This makes the
    // dead tombstone scanner in CleanableSegmentManager more efficient
    // since it will only need to scan the front of the segment.
    for (int tombstonePass = 1; tombstonePass >= 0 && !empty; tombstonePass--) {
        for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
            LogEntryType type = it.getType();

            if (tombstonePass && type != LOG_ENTRY_TYPE_OBJTOMB)
                continue;
            if (!tombstonePass && type == LOG_ENTRY_TYPE_OBJTOMB)
                continue;

            Buffer buffer;
            it.appendToBuffer(buffer);
            Log::Reference reference = segment->getReference(it.getOffset());
            uint32_t bytesAppended = 0;
            RelocStatus s = relocateEntry(type,
                                          buffer,
                                          reference,
                                          survivor,
                                          &localMetrics,
                                          &bytesAppended);
            if (expect_false(s == RELOCATION_FAILED))
                throw FatalError(HERE, "Entry didn't fit into survivor!");

            localMetrics.totalEntriesScanned[type]++;
            localMetrics.totalScannedEntryLengths[type] +=
                buffer.size();
            if (expect_true(s == RELOCATED)) {
                localMetrics.totalLiveEntriesScanned[type]++;
                localMetrics.totalLiveScannedEntryLengths[type] +=
                    buffer.size();
                liveScannedEntryTotalLengths[type] += bytesAppended;
            }
        }
    }

    // Be sure to update the usage statistics for the new segment. This
    // is done once here, rather than for each relocated entry because
    // it avoids the expense of atomically updating those fields.
    for (size_t i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
        survivor->trackNewEntries(static_cast<LogEntryType>(i),
                    downCast<uint32_t>(localMetrics.totalLiveEntriesScanned[i]),
                    liveScannedEntryTotalLengths[i]);
    }

    survivor->close();
    bool r = survivor->freeUnusedSeglets();
    if (!r) {
        assert(r);
    }
    assert(segment->getSegletsAllocated() >= survivor->getSegletsAllocated());

    uint64_t freeSegletsGained = segment->getSegletsAllocated() -
                                 survivor->getSegletsAllocated();
    if (freeSegletsGained == 0)
        balancer->compactionFailed();
    uint64_t bytesFreed = freeSegletsGained * segletSize;
    localMetrics.totalBytesFreed += bytesFreed;
    localMetrics.totalBytesAppendedToSurvivors +=
        survivor->getAppendedLength();
    localMetrics.totalSegmentsCompacted++;

    // Merge our local metrics into the global aggregate counters.
    inMemoryMetrics.merge(localMetrics);

    // Also, maintain a few key statistics in PerfStats.
    PerfStats::threadStats.compactorInputBytes +=
            localMetrics.totalBytesInCompactedSegments;
    PerfStats::threadStats.compactorSurvivorBytes +=
            localMetrics.totalBytesAppendedToSurvivors;
    PerfStats::threadStats.compactorActiveCycles +=
            Cycles::rdtsc() - startTicks;

    AtomicCycleCounter __(&inMemoryMetrics.compactionCompleteTicks);
    segmentManager.compactionComplete(segment, survivor);
}

/**
 * Perform a disk cleaning pass if possible. Doing so involves choosing segments
 * to clean, extracting entries from those segments, writing them out into new
 * "survivor" segments, and alerting the segment manager upon completion.
 */
void
LogCleaner::doDiskCleaning()
{
    TEST_LOG("called");
    AtomicCycleCounter _(&onDiskMetrics.totalTicks);
    uint64_t startTicks = Cycles::rdtsc();

    // Obtain the segments we'll clean in this pass. We're guaranteed to have
    // the resources to clean what's returned.
    LogSegmentVector segmentsToClean;
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0)
        return;

    LogCleanerMetrics::OnDisk<uint64_t> localMetrics;

    onDiskMetrics.memoryUtilizationAtStartSum +=
        segmentManager.getMemoryUtilization();

    // Extract the currently live entries of the segments we're cleaning and
    // sort them by age.
    EntryVector entries;
    getSortedEntries(segmentsToClean, entries, &localMetrics);

    uint64_t maxLiveBytes = 0;
    uint32_t segletsBefore = 0;
    foreach (LogSegment* segment, segmentsToClean) {
        uint64_t liveBytes = segment->getLiveBytes();
        if (liveBytes == 0)
            onDiskMetrics.totalEmptySegmentsCleaned++;
        maxLiveBytes += liveBytes;
        segletsBefore += segment->getSegletsAllocated();
    }

    // Relocate the live entries to survivor segments. Be sure to use local
    // counters and merge them into our global metrics afterwards to avoid
    // cache line ping-ponging in the hot path.
    LogSegmentVector survivors;
    uint64_t entryBytesAppended = relocateLiveEntries(entries, survivors,
            &localMetrics);

    uint32_t segmentsAfter = downCast<uint32_t>(survivors.size());
    uint32_t segletsAfter = 0;
    foreach (LogSegment* segment, survivors)
        segletsAfter += segment->getSegletsAllocated();

    TEST_LOG("used %u seglets and %u segments", segletsAfter, segmentsAfter);

    // If this doesn't hold, then our statistics are wrong. Perhaps
    // MasterService is issuing a log->free(), but is leaving a reference in
    // the hash table. Or perhaps objects or tombstones which were once
    // considered dead have come to life again.
    if (entryBytesAppended > maxLiveBytes) {
        assert(entryBytesAppended <= maxLiveBytes);
    }

    uint32_t segmentsBefore = downCast<uint32_t>(segmentsToClean.size());
    assert(segletsBefore >= segletsAfter);
    if (segmentsBefore < segletsAfter) {
        assert(segmentsBefore >= segmentsAfter);
    }

    uint64_t memoryBytesFreed = (segletsBefore - segletsAfter) * segletSize;
    uint64_t diskBytesFreed = (segmentsBefore - segmentsAfter) * segmentSize;
    localMetrics.totalMemoryBytesFreed += memoryBytesFreed;
    localMetrics.totalDiskBytesFreed += diskBytesFreed;
    localMetrics.totalSegmentsCleaned += segmentsToClean.size();
    localMetrics.totalSurvivorsCreated += survivors.size();
    localMetrics.totalRuns++;
    if (segmentManager.getSegmentUtilization() >= MIN_DISK_UTILIZATION)
        localMetrics.totalLowDiskSpaceRuns++;
    onDiskMetrics.lastRunTimestamp = WallTime::secondsTimestamp();
    onDiskMetrics.merge(localMetrics);

    // Also, maintain a few key statistics in PerfStats.
    PerfStats::threadStats.cleanerInputMemoryBytes +=
            localMetrics.totalMemoryBytesInCleanedSegments;
    PerfStats::threadStats.cleanerInputDiskBytes +=
            localMetrics.totalDiskBytesInCleanedSegments;
    // As of July, 2015, localMetrics.totalBytesAppendedToSurvivors
    // seems always to be zero.  Thus, recompute it using other metrics.
    PerfStats::threadStats.cleanerSurvivorBytes +=
            localMetrics.totalDiskBytesInCleanedSegments -
            localMetrics.totalDiskBytesFreed;
    PerfStats::threadStats.cleanerActiveCycles +=
            Cycles::rdtsc() - startTicks;

    AtomicCycleCounter __(&onDiskMetrics.cleaningCompleteTicks);
    segmentManager.cleaningComplete(segmentsToClean, survivors);

    return;
}

/**
 * Choose the best segment to clean in memory. We greedily choose the segment
 * with the most freeable seglets. Care is taken to ensure that we determine the
 * number of freeable seglets that will keep the segment under our maximum
 * cleanable utilization after compaction. This ensures that we will always be
 * able to use the compacted version of this segment during disk cleaning.
 */
LogSegment*
LogCleaner::getSegmentToCompact()
{
    AtomicCycleCounter _(&inMemoryMetrics.getSegmentToCompactTicks);
    return cleanableSegments.getSegmentToCompact();
}

/**
 * Compute the best segments to clean on disk and return a set of them that we
 * are guaranteed to be able to clean while consuming no more space in memory
 * than they currently take up.
 *
 * \param[out] outSegmentsToClean
 *      Vector in which segments chosen for cleaning are returned.
 * \return
 *      Returns the total number of seglets allocated in the segments chosen for
 *      cleaning.
 */
void
LogCleaner::getSegmentsToClean(LogSegmentVector& outSegmentsToClean)
{
    AtomicCycleCounter _(&onDiskMetrics.getSegmentsToCleanTicks);
    cleanableSegments.getSegmentsToClean(outSegmentsToClean);
}

/**
 * Sort the given segment entries by their timestamp. Used to sort the survivor
 * data that is written out to multiple segments during disk cleaning. This
 * helps to segregate data we expect to live longer from those likely to be
 * shorter lived, which in turn can reduce future cleaning costs.
 *
 * This happens to sort younger objects first, but the opposite should work just as
 * well.
 *
 * \param entries
 *      Vector containing the entries to sort.
 */
void
LogCleaner::sortEntriesByTimestamp(EntryVector& entries)
{
    AtomicCycleCounter _(&onDiskMetrics.timestampSortTicks);
    std::sort(entries.begin(), entries.end(), TimestampComparer());
}

/**
 * Extract a complete list of entries from the given segments we're going to
 * clean and sort them by age.
 *
 * \param segmentsToClean
 *      Vector containing the segments to extract entries from.
 * \param[out] outEntries
 *      Vector containing sorted live entries in the segment.
 * \param[out] localMetrics
 *      Contains various performance counters that are incremented here.
 */
void
LogCleaner::getSortedEntries(LogSegmentVector& segmentsToClean,
                             EntryVector& outEntries,
                             LogCleanerMetrics::OnDisk<uint64_t>* localMetrics)
{
    AtomicCycleCounter _(&onDiskMetrics.getSortedEntriesTicks);

    foreach (LogSegment* segment, segmentsToClean) {
        for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
            LogEntryType type = it.getType();
            Buffer buffer;
            it.appendToBuffer(buffer);
            uint32_t timestamp = entryHandlers.getTimestamp(type, buffer);
            outEntries.push_back(Entry(segment->getReference(it.getOffset()),
                                       timestamp));
        }
    }

    sortEntriesByTimestamp(outEntries);

    foreach (LogSegment* segment, segmentsToClean) {
        localMetrics->totalMemoryBytesInCleanedSegments +=
            segment->getSegletsAllocated() * segletSize;
        localMetrics->totalDiskBytesInCleanedSegments += segmentSize;
        onDiskMetrics.cleanedSegmentMemoryHistogram.storeSample(
            segment->getMemoryUtilization());
        onDiskMetrics.cleanedSegmentDiskHistogram.storeSample(
            segment->getDiskUtilization());
    }

    TEST_LOG("%lu entries extracted from %lu segments",
        outEntries.size(), segmentsToClean.size());
}

void LogCleaner::addObjectToTrack(uint64_t key, bool read, uint64_t callerId)
{
    
    //LogCleaner::ObjectTracker newObj(key,0,0,0);
    //Does object exists

    if(objectTracked.find(key)!=objectTracked.end())
    {
        TEST_LOG("Updating object %lu to ObjectTracker",key);
        ObjectTracker newObj = objectTracked[key];
        
        newObj.lasttimestamp = read ? WallTime::secondsTimestamp() : newObj.lasttimestamp;
        newObj.readCount = read ? newObj.readCount++ : newObj.readCount; 
        newObj.writeCount = !read ? newObj.writeCount++ : newObj.writeCount;

        objectTracked[key] = newObj; 
        delete &newObj; 
        return;
    }

    TEST_LOG("Adding object %lu to ObjectTracker", key);
    ObjectTracker newObj(WallTime::secondsTimestamp(),0,0, callerId);
    objectTracked[key] = newObj;
    delete &newObj;
    return; 

}

void LogCleaner::removeObjectToTrack(uint64_t key)
{
    objectTracked.erase(objectTracked.find(key));
}

/**
 * Return the amount of free memory available in percentage format
 */
bool LogCleaner::Balancer::freeMemoryAvail()
{
    return ( sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE) )
                    > ( ( sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE) ) * (LOWER_MEMORY_THRESHOLD/100) ) ;
}

/**
 * Given a vector of entries from segments being cleaned, write them out to
 * survivor segments in order and alert their owning module (MasterService,
 * usually), that they've been relocated.
 *
 * \param entries
 *      Vector the entries from segments being cleaned that may need to be
 *      relocated.
 * \param outSurvivors
 *      The new survivor segments created to hold the relocated live data are
 *      returned here.
 * \param[out] localMetrics
 *      Contains various performance counters that are incremented here.
 * \return
 *      The number of live bytes appended to survivors is returned. This value
 *      includes any segment metadata overhead. This makes it directly
 *      comparable to the per-segment liveness statistics that also include
 *      overhead.
 */
uint64_t
LogCleaner::relocateLiveEntries(EntryVector& entries,
                            LogSegmentVector& outSurvivors,
                            LogCleanerMetrics::OnDisk<uint64_t>* localMetrics)
{
    CycleCounter<uint64_t> _(&localMetrics->relocateLiveEntriesTicks);

    LogSegment* survivor = NULL;
    uint64_t totalEntryBytesAppended = 0;
    uint32_t currentLiveEntries[TOTAL_LOG_ENTRY_TYPES] = { 0 };
    uint32_t currentLiveEntryLengths[TOTAL_LOG_ENTRY_TYPES] = { 0 };

    foreach (Entry& entry, entries) {
        Buffer buffer;
        LogEntryType type = entry.reference.getEntry(
            &segmentManager.getAllocator(), &buffer);
        Log::Reference reference = entry.reference;
        uint32_t bytesAppended = 0;
        RelocStatus s = relocateEntry(type,
                                      buffer,
                                      reference,
                                      survivor,
                                      localMetrics,
                                      &bytesAppended);

        if (expect_false(s == RELOCATION_FAILED)) {
            if (survivor != NULL) {
                for (size_t i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
                    survivor->trackNewEntries(static_cast<LogEntryType>(i),
                                              currentLiveEntries[i],
                                              currentLiveEntryLengths[i]);
                }
                memset(currentLiveEntries, 0, sizeof(currentLiveEntries));
                memset(currentLiveEntryLengths, 0,
                       sizeof(currentLiveEntryLengths));
                closeSurvivor(survivor);
            }

            // Allocate a survivor segment to write into. This call may block if
            // one is not available right now.
            CycleCounter<uint64_t> waitTicks(
                &localMetrics->waitForFreeSurvivorsTicks);
            survivor = segmentManager.allocSideSegment(
                SegmentManager::FOR_CLEANING | SegmentManager::MUST_NOT_FAIL,
                NULL);
            assert(survivor != NULL);
            waitTicks.stop();
            outSurvivors.push_back(survivor);

            s = relocateEntry(type,
                              buffer,
                              reference,
                              survivor,
                              localMetrics,
                              &bytesAppended);
            if (s == RELOCATION_FAILED)
                throw FatalError(HERE, "Entry didn't fit into empty survivor!");
        }

        localMetrics->totalEntriesScanned[type]++;
        localMetrics->totalScannedEntryLengths[type] += buffer.size();
        if (expect_true(s == RELOCATED)) {
            localMetrics->totalLiveEntriesScanned[type]++;
            localMetrics->totalLiveScannedEntryLengths[type] +=
                buffer.size();
            currentLiveEntries[type]++;
            currentLiveEntryLengths[type] += bytesAppended;
        }

        totalEntryBytesAppended += bytesAppended;
    }

    if (survivor != NULL) {
        for (size_t i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
            survivor->trackNewEntries(static_cast<LogEntryType>(i),
                                      currentLiveEntries[i],
                                      currentLiveEntryLengths[i]);
        }
        closeSurvivor(survivor);
    }

    // Ensure that the survivors have been synced to backups before proceeding.
    double survivorMb = static_cast<double>(totalEntryBytesAppended);
    survivorMb /= 1e06;
    uint64_t start = Cycles::rdtsc();
    foreach (survivor, outSurvivors) {
        CycleCounter<uint64_t> __(&localMetrics->survivorSyncTicks);
        survivor->replicatedSegment->sync(survivor->getAppendedLength());
    }
    double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
    LOG(NOTICE, "Cleaner finished syncing survivor segments: %.1f ms, "
            "%.1f MB/sec", elapsed*1e03, survivorMb/elapsed);

    return totalEntryBytesAppended;
}

/**
 * Close a survivor segment we've written data to as part of a disk cleaning
 * pass and tell the replicaManager to begin flushing it asynchronously to
 * backups. Any unused seglets in the survivor will will be freed for use in
 * new segments.
 *
 * \param survivor
 *      The new disk segment we've written survivor data to.
 */
void
LogCleaner::closeSurvivor(LogSegment* survivor)
{
    AtomicCycleCounter _(&onDiskMetrics.closeSurvivorTicks);
    onDiskMetrics.totalBytesAppendedToSurvivors +=
        survivor->getAppendedLength();

    survivor->close();

    // Once the replicatedSegment is told that the segment is closed, it will
    // begin replicating the contents. By closing survivors as we go, we can
    // overlap backup writes with filling up new survivors.
    survivor->replicatedSegment->close();

    // Immediately free any unused seglets.
    bool r = survivor->freeUnusedSeglets();
    if (!r) {
        assert(r);
    }
}

bool
LogCleaner::Balancer::isMemoryLow(CleanerThreadState* thread)
{
    // T = Total % of memory in use (including tombstones, dead objects, etc).
    // L = Total % of memory in use by live objects.
    const int T = cleaner->segmentManager.getMemoryUtilization();
    const int L = cleaner->cleanableSegments.getLiveObjectUtilization();

    // We need to clean if memory is low and there's space that could be
    // reclaimed. It's not worth cleaning if almost everything is alive.
    int baseThreshold = std::max(90, (100 + L) / 2);
    if (T < baseThreshold)
        return false;

    // Employ multiple threads only when we fail to keep up with fewer of them.
    if (thread->threadNumber > 0) {
        int thresh = baseThreshold + 2 * static_cast<int>(thread->threadNumber);
        if (T < std::min(99, thresh))
            return false;
    }

    return true;
}

/**
 * This method is called by the memory compactor if it failed to free any memory
 * after processing a segment. This is a pretty good signal that it might be
 * time to run the disk cleaner.
 */
void
LogCleaner::Balancer::compactionFailed()
{
    compactionFailures++;
}
LogCleaner::Balancer::CleaningTask
LogCleaner::Balancer::needsGC(GCThreadState* thread)
{
    //Do we need GC?
    if(isGCTime(thread))
        return GC;
    //Do we need to evict Memory
    if(!freeMemoryAvail()) 
        return EVICT_MEM;

    //Do we need to increase mem 
    if(isTimeIncreaseMem(thread))
        return INCREASE_MEM;

    return SLEEP;
}

LogCleaner::Balancer::CleaningTask
LogCleaner::Balancer::requestTask(CleanerThreadState* thread)
{
    if (isDiskCleaningNeeded(thread))
        return CLEAN_DISK;

    if (!cleaner->disableInMemoryCleaning && isMemoryLow(thread))
        return COMPACT_MEMORY;

    return SLEEP;
}

LogCleaner::TombstoneRatioBalancer::TombstoneRatioBalancer(LogCleaner* cleaner,
                                                           double ratio)
    : Balancer(cleaner)
    , ratio(ratio)
{
    LOG(NOTICE, "Using tombstone ratio balancer with ratio = %f", ratio);
    if (ratio < 0 || ratio > 1)
        DIE("Invalid tombstoneRatio argument (%f). Must be in [0, 1].", ratio);
}

LogCleaner::TombstoneRatioBalancer::~TombstoneRatioBalancer()
{
}

bool
LogCleaner::TombstoneRatioBalancer::isDiskCleaningNeeded(
                                                    CleanerThreadState* thread)
{
    // Our disk cleaner is fast enough to chew up considerable backup bandwidth
    // with just one thread. If we're running with backups, then only permit
    // one thread to clean on disk.
    if (thread->threadNumber != 0 && !cleaner->disableInMemoryCleaning)
        return false;

    // If we're running out of disk space, we need to run the disk cleaner.
    if (cleaner->segmentManager.getSegmentUtilization() >= MIN_DISK_UTILIZATION)
        return true;

    // If we're not low on memory then we need not clean. Note that this is
    // purposefully checked after first seeing if we're low on disk space.
    if (!isMemoryLow(thread))
        return false;

    // If we are low on memory, but the compactor is disabled, we must clean.
    if (cleaner->disableInMemoryCleaning)
        return true;

    // If the ratio is set high and the memory utilisation is also high, it's
    // possible that we won't have a large enough percentage of tombstones to
    // trigger disk cleaning, but will also not gain anything from compacting.
    if (compactionFailures > compactionFailuresHandled) {
        compactionFailuresHandled++;
        return true;
    }

    // We're low on memory and the compactor is enabled. We'll let the compactor
    // do its thing until tombstones start to pile up enough that running the
    // disk cleaner is needed to free some of them (by discarding the some of
    // the segments they refer to).
    //
    // The heuristic we'll use to determine when tombstones are piling up is
    // as follows: if tombstones that may be alive account for at least X% of
    // the space not used by live objects (that is, space that is eventually
    // reclaimable), then we should run the disk cleaner to hopefully make some
    // of them dead.
    const int U = cleaner->cleanableSegments.getUndeadTombstoneUtilization();
    const int L = cleaner->cleanableSegments.getLiveObjectUtilization();
    if (U >= static_cast<int>(ratio * (100 - L)))
        return true;

    return false;
}

LogCleaner::FixedBalancer::FixedBalancer(LogCleaner* cleaner,
                                         uint32_t cleaningPercentage)
    : Balancer(cleaner)
    , cleaningPercentage(cleaningPercentage)
{
    if (cleaner->disableInMemoryCleaning && cleaningPercentage < 100) {
        DIE("Memory compaction disabled, but wanted %d%% compaction!?",
            100 - cleaningPercentage);
    }
    LOG(NOTICE, "Using fixed balancer with %u%% disk cleaning",
        cleaningPercentage);
}

LogCleaner::FixedBalancer::~FixedBalancer()
{
}

bool
LogCleaner::FixedBalancer::isDiskCleaningNeeded(CleanerThreadState* thread)
{
    // See TombstoneRatioBalancer::isDiskCleaningNeeded for comments on this
    // first handful of conditions.
    if (thread->threadNumber != 0)
        return false;

    if (cleaner->segmentManager.getSegmentUtilization() >= MIN_DISK_UTILIZATION)
        return true;

    if (!isMemoryLow(thread))
        return false;

    // If the memory compactor has failed to free any space recent, it's a good
    // sign that we need disk cleaning.
    if (compactionFailures > compactionFailuresHandled) {
        compactionFailuresHandled++;
        return true;
    }

    uint64_t diskTicks = thread->diskCleaningTicks;
    uint64_t memoryTicks = thread->memoryCompactionTicks;
    uint64_t totalTicks = diskTicks + memoryTicks;

    if (totalTicks == 0)
        return true;

    if (100 * diskTicks / totalTicks > cleaningPercentage)
        return false;

    return true;
}

/**
 * Construct a Disabler object. Once the constructor returns, the caller
 * can be certain that no cleaner threads are running, or will run until
 * the object is destroyed.
 *
 * \param cleaner
 *      Identifies the cleaner that should be disabled.
 */
LogCleaner::Disabler::Disabler(LogCleaner* cleaner)
    : cleaner(cleaner)
{
    Lock lock(cleaner->mutex);
    cleaner->disableCount++;
    while (cleaner->activeThreads) {
        cleaner->cleanerIdle.wait(lock);
    }
}

/**
 * Destroy a Disabler object. Once all Disablers have been destroyed, log
 * cleaning can resume.
 */
LogCleaner::Disabler::~Disabler()
{
    Lock lock(cleaner->mutex);
    cleaner->disableCount--;
}

} // namespace
