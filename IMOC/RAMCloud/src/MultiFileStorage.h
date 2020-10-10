/* Copyright (c) 2010-2016 Stanford University
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

#ifndef RAMCLOUD_MULTIFILESTORAGE_H
#define RAMCLOUD_MULTIFILESTORAGE_H

#include <stack>

#include "Common.h"
#include "BackupStorage.h"
#include "PriorityTaskQueue.h"

namespace RAMCloud {

/**
 * A BackupStorage backend which divides replica data between one or more files
 * or disk devices. Clients interact with this class primarily through Frame
 * objects (see Frame documentation below). Using multiple disks provides more
 * storage capacity and faster read/write speeds than a single disk.
 */
class MultiFileStorage : public BackupStorage {
  public:
    /// See bufferDeleter below.
    struct BufferDeleter {
        explicit BufferDeleter(MultiFileStorage* storage);
        void operator()(void* buffer);

        /// MultiFileStorage which houses the pool where buffers are returned.
        MultiFileStorage* storage;
    };

    typedef std::unique_ptr<void, BufferDeleter> BufferPtr;

    /**
     * Represents both in-memory and on-disk storage of a replica. After opened,
     * a Frame remains associated with the same replica for the lifetime of that
     * replica. Frames are divided into framelets, which are the units that
     * exist on individual files. See MultiFileStorage::bytesInFramelet() for
     * information on how Frames are divided into framelets.
     */
    class Frame : public BackupStorage::Frame
                , public PriorityTask
    {
      PUBLIC:
        typedef std::unique_lock<std::mutex> Lock;

        Frame(MultiFileStorage* storage, size_t frameIndex);
        ~Frame();

        void schedule(Priority priority);
        void schedule(Lock& lock, Priority priority);

        bool wasAppendedToByCurrentProcess();

        void loadMetadata();
        const void* getMetadata();

        void startLoading();
        bool isLoaded();
        bool currentlyOpen() { return isOpen;}
        void* load();
        void unload();

        void append(Buffer& source,
                    size_t sourceOffset,
                    size_t length,
                    size_t destinationOffset,
                    const void* metadata,
                    size_t metadataLength);
        void close();
        void reopen(size_t length);
        void free();

        void performTask();

      PRIVATE:
        void open(bool sync, ServerId masterId, uint64_t segmentId);

        void performRead(Lock& lock);
        void performWrite(Lock& lock);

        bool isSynced() const;

        /// Storage where this frame resides.
        MultiFileStorage* storage;

        /**
         * Identifies a frame in storage. See offsetOfFramelet() for details on
         * how this index is used to map to regions of the storage files.
         */
        const size_t frameIndex;

        /**
         * Identifier for the master server whose log contains the segment
         * associated with this replica.
         */
        ServerId masterId;

        /**
         * Segment number of the segment associated with this replica.
         */
        uint64_t segmentId;

        /**
         * Buffer where data is staged for writes and loaded into on load().
         * Reset at various points to release memory when the buffer isn't
         * going to be immediately needed. Some care is needed with #buffer
         * since it must stay valid even during performWrite() even though
         * the lock isn't held for the duration of the call.
         */
        BufferPtr buffer;

        /**
         * Tracks whether a replica has been opened (either initially or
         * since the time of the last free). False if #isClosed.
         */
        bool isOpen;

        /**
         * Tracks whether a replica has been closed (either initially or
         * since the time of the last free). False if #isOpen.
         */
        bool isClosed;

        /**
         * True if calls to append() should block until appended data and
         * metadata has been written to storage.
         */
        bool sync;

        /**
         * True means this frame is/was used for accumulating new data
         * in a head segment (i.e. it counts in writeBuffersInUse, so
         * writeBuffersInUse must be decremented when buffer is freed).
         * False means this frame was used for reading data during
         * crash recovery.
         */
        bool isWriteBuffer;

        /**
         * Tracks whether append has been called on this frame during the
         * life of this process. This includes across free()/open() cycles.
         * This is used by the backup replica garbage collector to determine
         * whether it might have missed a BackupFree rpc from a master, in
         * which case it has to query the master for the replica status.
         * This is "appended to" rather than "opened by" because of benchmark();
         * benchmark "opens" replicas and loads them but doesn't do any.
         * Masters open and append data in a single rpc, so this is a fine
         * proxy.
         */
        bool appendedToByCurrentProcess;

        /// Bytes appended to the replica so far.
        size_t appendedLength;

        /// Of #appendedLength how much has been stored durably.
        size_t committedLength;

        /**
         * Metadata given on the most recent call to append. Starts zeroed
         * on construction. Reset to the metadata stored on disk is
         * loadMetadata() is called (only safe before doing any other
         * operations).
         */
        Memory::unique_ptr_free appendedMetadata;

        /// Bytes of #appendedMetadata that contain valid data to be preserved.
        size_t appendedMetadataLength;

        /* Revision number for the metadata that was appeneded. Used to ensure
         * that the metadata updates that happen while prior metadata is being
         * flushed to storage eventually make it to storage.
         */
        uint64_t appendedMetadataVersion;

        /// Revision number of metadata block most recently flushed to storage.
        uint64_t committedMetadataVersion;

        /**
         * True if the replica data has been requested. Will cause IO if the
         * replica is no longer in memory.
         */
        bool loadRequested;

        /// True if a read or write is ongoing (which is done without a lock).
        bool performingIo;

        /**
         * Logical timestamp used to track which lifecycle of the frame io was
         * scheduled during. If a task is scheduled and then freed this can be
         * used to detect that invocations of the task after the free shouldn't
         * actually perform io.
         */
        uint64_t epoch;

        /**
         * Timestamp indicating the last time schedule() was called. If the
         * epoch has changed when the task is finally invoked then the frame
         * has been freed for reuse and the io is skipped.
         */
        uint64_t scheduledInEpoch;

        /// Used for testing.
        bool testingHadToWaitForBufferOnLoad;
        bool testingHadToWaitForSyncOnLoad;
        static bool testingSkipRealIo;

        // ONLY for open() and isSynced(); please try not to touch other
        // details of frames in MultiFileStorage (or elsewhere).
        friend class MultiFileStorage;
        DISALLOW_COPY_AND_ASSIGN(Frame);
    };

    MultiFileStorage(size_t segmentSize,
                     size_t frameCount,
                     size_t writeRateLimit,
                     size_t maxNonVolatileBuffers,
                     const char* filePaths,
                     int openFlags = 0);
    ~MultiFileStorage();

    FrameRef open(bool sync, ServerId masterId, uint64_t segmentId);
    uint32_t benchmark(BackupStrategy backupStrategy);
    size_t getMetadataSize();
    std::vector<FrameRef> loadAllMetadata();
    void resetSuperblock(ServerId serverId,
                                 const string& clusterName,
                                 uint32_t frameSkipMask = 0);
    Superblock loadSuperblock();
    void quiesce();
    void fry();

    BufferPtr allocateBuffer();

    /**
     * Internal use only; block size of storage. Needed to deal
     * with alignment constraints for O_DIRECT.
     */
    enum { BLOCK_SIZE = 512 };
    static_assert(sizeof(Superblock) < BLOCK_SIZE,
                  "Superblock doesn't fit in a single disk block");
    /**
     * Maximum size of metadata for each frame.
     * Must meet alignement constraints of O_DIRECT.
     * Only public so InMemoryStorage can use the same metadata
     * size limit.
     */
    enum { METADATA_SIZE = BLOCK_SIZE };

  PRIVATE:
    size_t bytesInFramelet(size_t fileIndex) const;
    off_t offsetOfFramelet(size_t frameIndex) const;
    off_t offsetOfFrameMetadata(size_t frameIndex) const;
    off_t offsetOfSuperblockFrame(size_t superblockIndex) const;
    void unlockedRead(Frame::Lock& lock, void* buf, size_t frameIndex,
                      bool usingDevNull);
    void unlockedWrite(Frame::Lock& lock, void* buf, size_t count,
                       size_t frameIndex, off_t offsetInFrame,
                       void* metadataBuf, size_t metadataCount);

    void reserveSpace(int fd);
    Tub<Superblock> tryLoadSuperblock(uint32_t superblockFrame);

    /// Protects concurrent operations on storage and all of its frames.
    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /**
     * Orders competing read/write operations for frames and calls back
     * to frames when their turn for IO arrives. Provides its own
     * thread which is dedicated to and serializes IO.
     */
    PriorityTaskQueue ioQueue;

    /// Holds the most recent image of the superblock.
    Superblock superblock;

    /// Tracks which of the superblock frames was most recently written.
    uint32_t lastSuperblockFrame;

    /**
     * A frame for each a region of the files on storage which hold a replica.
     * Frames get reused for different replicas making a frame something of a
     * state machine, but are all created and destroyed along with the
     * storage instance. See bytesInFramelet() for details on how frames are
     * divided between files.
     */
    std::deque<Frame> frames;

    /// The number of replicas this storage can store simultaneously.
    const size_t frameCount;

    /// Type of the freeMap.  A bitmap.
    typedef boost::dynamic_bitset<> FreeMap;
    /// Keeps a bit set for each frame in frames indicating if it is free.
    FreeMap freeMap;

    /**
     * Track the last used segment frame so they can be used in FIFO.
     * This gives recovery dump tools a much better chance at recovering
     * data since old data is destroyed from disk first rather than new.
     */
    FreeMap::size_type lastAllocatedFrame;

    /// Extra flags for use while opening filePath (e.g. O_DIRECT | O_SYNC).
    int openFlags;

    /**
     * The file descriptors of the storage files. See bytesInFramelet() for
     * details on how data is divided between files.
     */
    std::vector<int> fds;

    /// Set to true if the filePath issued to the constructor was "/dev/null".
    /// We need to keep track of this since /dev/null will readily take any
    /// bytes written to it, but does not return anything, which breaks the
    /// initial "disk" benchmark.
    const bool usingDevNull;

    /**
     * Tracks number of non-volatile buffers currently in use for data
     * coming in from masters while (a) replicas are filling and (b) they
     * are getting written to storage.  Used to throttle the creation
     * of new replicas if too much unwritten data accumulates. Note: this
     * affects only write buffers: it doesn't limit buffers allocated
     * during recovery to read replicas.
     */
    size_t writeBuffersInUse;

    /**
     * Upper limit on writeBuffersInUse.
     */
    size_t maxWriteBuffers;

    /**
     * Returns buffers allocated with MultiFileStorage::allocateBuffer()
     * to a pool, or if there are already plenty of buffers
     * it returns it to the OS (which will unmap it).
     * Should only be called by MultiFileStorage::BufferPtr objects.
     */
    BufferDeleter bufferDeleter;

    /**
     * Pool of unused buffers which the backup uses to stage new segment
     * replicas in and to load replicas from disk into. This pool is
     * managed by allocateBuffer() and #bufferDeleter.
     */
    std::stack<void*, std::vector<void*>> buffers;

    DISALLOW_COPY_AND_ASSIGN(MultiFileStorage);
};

} // namespace RAMCloud

#endif
