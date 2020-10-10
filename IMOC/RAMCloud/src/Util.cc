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

#include <sstream>

#include "Util.h"
#include "Cycles.h"
#include "Logger.h"

namespace RAMCloud {
namespace Util {

// Memory buffer used by spinAndCheckGaps.
#define SPIN_BUFFER_SIZE 10000
char spinBuffer[SPIN_BUFFER_SIZE];

/**
 * Sets the allowable set of cores for the current threadto include
 * all of the available processors. It is used to restore a previously
 * restricted affinity set back to the default (i.e. clears any
 * affinity setting).
 */
void
clearCpuAffinity(void)
{
    cpu_set_t cpuSet;
    int numCpus;

    numCpus = downCast<int>(sysconf(_SC_NPROCESSORS_ONLN));
    CPU_ZERO(&cpuSet);
    for (int cpu = 0; cpu < numCpus; cpu++) {
        CPU_SET(cpu, &cpuSet);
    }
    if (sched_setaffinity(0, sizeof(cpuSet), &cpuSet) != 0) {
        RAMCLOUD_LOG(ERROR, "sched_setaffinitity failed: %s", strerror(errno));
    };
}

/**
 * Returns a string describing the CPU affinity of the current thread.
 * Each character in the string corresponds to a core; "X" means the
 * thread can run on that core, "-" means it cannot.
 */
string
getCpuAffinityString(void)
{
    cpu_set_t cpuSet;
    int numCpus;

    numCpus = downCast<int>(sysconf(_SC_NPROCESSORS_ONLN));
    if (sched_getaffinity(0, sizeof(cpuSet), &cpuSet) != 0) {
        RAMCLOUD_LOG(ERROR, "sched_getaffinitity failed: %s", strerror(errno));
    };
    string result;
    for (int cpu = 0; cpu < numCpus; cpu++) {
        result.append(CPU_ISSET(cpu, &cpuSet) ? "X" : "-");
    }
    return result;
}

/**
 * Generate a random string.
 *
 * \param str
 *      Pointer to location where the string generated will be stored.
 * \param length
 *      Length of the string to be generated in bytes.
 */
void
genRandomString(char* str, const int length) {
    static const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < length; ++i) {
        str[i] = alphanum[generateRandom() % (sizeof(alphanum) - 1)];
    }
}

/**
 * Return (potentially multi-line) string hex dump of a binary buffer in
 * 'hexdump -C' style.
 * Note that this exceeds 80 characters due to 64-bit offsets.
 */
string
hexDump(const void *buf, uint64_t bytes)
{
    const unsigned char *cbuf = reinterpret_cast<const unsigned char *>(buf);
    uint64_t i, j;

    std::ostringstream output;
    for (i = 0; i < bytes; i += 16) {
        char offset[17];
        char hex[16][3];
        char ascii[17];

        snprintf(offset, sizeof(offset), "%016" PRIx64, i);
        offset[sizeof(offset) - 1] = '\0';

        for (j = 0; j < 16; j++) {
            if ((i + j) >= bytes) {
                snprintf(hex[j], sizeof(hex[0]), "  ");
                ascii[j] = '\0';
            } else {
                snprintf(hex[j], sizeof(hex[0]), "%02x",
                    cbuf[i + j]);
                hex[j][sizeof(hex[0]) - 1] = '\0';
                if (isprint(static_cast<int>(cbuf[i + j])))
                    ascii[j] = cbuf[i + j];
                else
                    ascii[j] = '.';
            }
        }
        ascii[sizeof(ascii) - 1] = '\0';

        output <<
            format("%s  %s %s %s %s %s %s %s %s  %s %s %s %s %s %s %s %s  "
                   "|%s|\n", offset, hex[0], hex[1], hex[2], hex[3], hex[4],
                   hex[5], hex[6], hex[7], hex[8], hex[9], hex[10], hex[11],
                   hex[12], hex[13], hex[14], hex[15], ascii);
    }
    return output.str();
}

/**
 * This method has been used during performance testing. It executes
 * in a tight loop copying small blocks of memory (anything to consume
 * CPU cycles), and checks for gaps in execution that indicate the
 * thread has been descheduled. It prints information for any gaps
 * that occur.
 * \param count
 *      Number of iterations to execute before returning.
 */
void spinAndCheckGaps(int count)
{
    RAMCLOUD_LOG(NOTICE, "Spinner starting");
    uint64_t prev = Cycles::rdtsc();
    uint64_t tooLong = Cycles::fromNanoseconds(50000);
    uint64_t lastGap = prev;
    for (int i = 0; i < count; i++) {
        int start = downCast<int>(generateRandom() % (SPIN_BUFFER_SIZE - 100));
        int end = downCast<int>(generateRandom() % (SPIN_BUFFER_SIZE - 100));
        memcpy(&spinBuffer[end], &spinBuffer[start], 50);
        uint64_t current = Cycles::rdtsc();
        if ((current - prev) >= tooLong) {
            RAMCLOUD_LOG(NOTICE, "Spinner gap of %.1f us (%.2f ms since "
                    "last gap, iteration %i)x",
                    Cycles::toSeconds(current - prev)*1e06,
                    Cycles::toSeconds(current-lastGap)*1e03,
                    i);
            lastGap = current;
        }
        prev = current;
    }
    RAMCLOUD_LOG(NOTICE, "Spinner done");
}

/**
 * Returns true if timespec \c t1 refers to an earlier time than \c t2.
 *
 * \param t1
 *      First timespec to compare.
 * \param t2
 *      Second timespec to compare.
 */
bool
timespecLess(const struct timespec& t1, const struct timespec& t2)
{
    return (t1.tv_sec < t2.tv_sec) ||
            ((t1.tv_sec == t2.tv_sec) && (t1.tv_nsec < t2.tv_nsec));
}

/**
 * Returns true if timespec \c t1 refers to an earlier time than
 * \c t2 or the same time.
 *
 * \param t1
 *      First timespec to compare.
 * \param t2
 *      Second timespec to compare.
 */
bool
timespecLessEqual(const struct timespec& t1, const struct timespec& t2)
{
    return (t1.tv_sec < t2.tv_sec) ||
            ((t1.tv_sec == t2.tv_sec) && (t1.tv_nsec <= t2.tv_nsec));
}

/**
 * Return the sum of two timespecs.  The timespecs do not need to be
 * normalized (tv_nsec < 1e09), but the result will be.
 *
 * \param t1
 *      First timespec to add.
 * \param t2
 *      Second timespec to add.
 */
struct timespec
timespecAdd(const struct timespec& t1, const struct timespec& t2)
{
    struct timespec result;
    result.tv_sec = t1.tv_sec + t2.tv_sec;
    uint64_t nsec = t1.tv_nsec + t2.tv_nsec;
    result.tv_sec += nsec/1000000000;
    result.tv_nsec = nsec%1000000000;
    return result;
}

/** 
 * Used for testing: if nonzero then this will be returned as the result of the
 * next call to read_pmc().
 */
uint64_t mockPmcValue = 0;

} // namespace Util
} // namespace RAMCloud
