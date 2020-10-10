/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_MULTIWRITE_H
#define RAMCLOUD_MULTIWRITE_H

#include "MultiOp.h"

namespace RAMCloud {

class MultiWrite : public MultiOp {
    static const WireFormat::MultiOp::OpType type =
                                        WireFormat::MultiOp::OpType::WRITE;

  PUBLIC:
    MultiWrite(RamCloud* ramcloud, MultiWriteObject* const requests[],
               uint32_t numRequests);

  PROTECTED:
    void appendRequest(MultiOpObject* request, Buffer* buf);
    bool readResponse(MultiOpObject* request, Buffer* response,
                      uint32_t* respOffset);
};
} // end RAMCloud

#endif /* MULTIWRITE_H */
