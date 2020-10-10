/* Copyright (c) 2010-2016 Stanford University
 * Copyright (c) 2011 Facebook
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

#include "BasicTransport.h"
#include "CycleCounter.h"
#include "HomaTransport.h"
#include "OptionParser.h"
#include "ShortMacros.h"
#include "RawMetrics.h"
#include "TransportManager.h"
#include "TransportFactory.h"
#include "TcpTransport.h"
#include "UdpDriver.h"
#include "FailSession.h"
#include "WorkerManager.h"
#include "WorkerSession.h"

#ifdef DPDK
#include "DpdkDriver.h"
#endif

#ifdef INFINIBAND
#include "InfRcTransport.h"
#include "InfUdDriver.h"
#endif

#ifdef ONLOAD
#include "SolarFlareDriver.h"
#endif

namespace RAMCloud {

static struct TcpTransportFactory : public TransportFactory {
    TcpTransportFactory()
        : TransportFactory("kernelTcp", "tcp") {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        return new TcpTransport(context, localServiceLocator);
    }
} tcpTransportFactory;

static struct BasicUdpTransportFactory : public TransportFactory {
    BasicUdpTransportFactory()
        : TransportFactory("basic+kernelUdp", "basic+udp") {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        return new BasicTransport(context, localServiceLocator,
                new UdpDriver(context, localServiceLocator), true,
                generateRandom());
    }
} basicUdpTransportFactory;

static struct HomaUdpTransportFactory : public TransportFactory {
    HomaUdpTransportFactory()
        : TransportFactory("homa+kernelUdp", "homa+udp") {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        return new HomaTransport(context, localServiceLocator,
                new UdpDriver(context, localServiceLocator), true,
                generateRandom());
    }
} homaUdpTransportFactory;

#ifdef ONLOAD
static struct BasicSolarFlareTransportFactory : public TransportFactory {
    BasicSolarFlareTransportFactory()
        : TransportFactory("basic+solarflare", "basic+sf") {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        return new BasicTransport(context, localServiceLocator,
                new SolarFlareDriver(context, localServiceLocator), true,
                generateRandom());
    }
} basicSolarFlareTransportFactory;

static struct HomaSolarFlareTransportFactory : public TransportFactory {
    HomaSolarFlareTransportFactory()
        : TransportFactory("homa+solarflare", "homa+sf") {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        return new HomaTransport(context, localServiceLocator,
                new SolarFlareDriver(context, localServiceLocator), true,
                generateRandom());
    }
} homaSolarFlareTransportFactory;
#endif

#ifdef INFINIBAND
static struct BasicInfUdTransportFactory : public TransportFactory {
    BasicInfUdTransportFactory()
        : TransportFactory("basic+infinibandud", "basic+infud") {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        return new BasicTransport(context, localServiceLocator,
                new InfUdDriver(context, localServiceLocator), true,
                generateRandom());
    }
} basicInfUdTransportFactory;

static struct HomaInfUdTransportFactory : public TransportFactory {
    HomaInfUdTransportFactory()
        : TransportFactory("homa+infinibandud", "homa+infud") {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        return new HomaTransport(context, localServiceLocator,
                new InfUdDriver(context, localServiceLocator), true,
                generateRandom());
    }
} homaInfUdTransportFactory;

static struct InfRcTransportFactory : public TransportFactory {
    InfRcTransportFactory()
        : TransportFactory("infinibandrc", "infrc") {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        return new InfRcTransport(context, localServiceLocator,
                generateRandom());
    }
} infRcTransportFactory;
#endif

#ifdef DPDK
struct BasicDpdkTransportFactory : public TransportFactory {
    BasicDpdkTransportFactory()
        : TransportFactory("basic+dpdk"), driver(NULL)  {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        if (driver == NULL) {
            LOG(WARNING, "Tried to use basic+dpdk transport, but DPDK is "
                    "not enabled (did you specify the --dpdkPort "
                    "command-line option?)");
            throw TransportException(HERE, "DPDK is not enabled");
        }
        return new BasicTransport(context, localServiceLocator, driver, false,
                generateRandom());
    }
    void setDpdkDriver(DpdkDriver* driver) {
        this->driver = driver;
    }
    DpdkDriver* driver;
    DISALLOW_COPY_AND_ASSIGN(BasicDpdkTransportFactory);
};
static BasicDpdkTransportFactory basicDpdkTransportFactory;

struct HomaDpdkTransportFactory : public TransportFactory {
    HomaDpdkTransportFactory()
        : TransportFactory("homa+dpdk"), driver(NULL)  {}
    Transport* createTransport(Context* context,
            const ServiceLocator* localServiceLocator) {
        if (driver == NULL) {
            LOG(WARNING, "Tried to use homa+dpdk transport, but DPDK is "
                    "not enabled (did you specify the --dpdkPort "
                    "command-line option?)");
            throw TransportException(HERE, "DPDK is not enabled");
        }
        return new HomaTransport(context, localServiceLocator, driver, false,
                generateRandom());
    }
    void setDpdkDriver(DpdkDriver* driver) {
        this->driver = driver;
    }
    DpdkDriver* driver;
    DISALLOW_COPY_AND_ASSIGN(HomaDpdkTransportFactory);
};
static HomaDpdkTransportFactory homaDpdkTransportFactory;
#endif

/**
 * TransportManager constructor.
 * 
 * \param context
 *      Shared state about various RAMCloud modules.
 */
TransportManager::TransportManager(Context* context)
    : context(context)
    , dpdkDriver()
    , isServer(false)
    , transportFactories()
    , transports()
    , listeningLocators()
    , sessionCache()
    , registeredBases()
    , registeredSizes()
    , mutex("TransportManager::mutex")
    , sessionTimeoutMs(0)
    , mockRegistrations(0)
{
    transportFactories.push_back(&tcpTransportFactory);
    transportFactories.push_back(&basicUdpTransportFactory);
    transportFactories.push_back(&homaUdpTransportFactory);
#ifdef ONLOAD
    transportFactories.push_back(&basicSolarFlareTransportFactory);
    transportFactories.push_back(&homaSolarFlareTransportFactory);
#endif
#ifdef INFINIBAND
    transportFactories.push_back(&basicInfUdTransportFactory);
    transportFactories.push_back(&homaInfUdTransportFactory);
    transportFactories.push_back(&infRcTransportFactory);
#endif
#ifdef DPDK
    transportFactories.push_back(&basicDpdkTransportFactory);
    transportFactories.push_back(&homaDpdkTransportFactory);
    if (context->options != NULL) {
        int dpdkPort = context->options->getDpdkPort();
        if (dpdkPort >= 0) {
            dpdkDriver = new DpdkDriver(context, dpdkPort);
            basicDpdkTransportFactory.setDpdkDriver(dpdkDriver);
            homaDpdkTransportFactory.setDpdkDriver(dpdkDriver);
        }
    }
#endif
    transports.resize(transportFactories.size(), NULL);
    if (context->options != NULL) {
        sessionTimeoutMs = context->options->getSessionTimeout();
    }
}

TransportManager::~TransportManager()
{
    // Must clear the cache and destroy sessionRefs before the
    // transports are destroyed.
    sessionCache.clear();

    // Delete any mockRegistrations
#if TESTING
    while (mockRegistrations > 0)
        unregisterMock();
#endif
    for (Transport* transport : transports) {
        delete transport;
    }

#ifdef DPDK
    if (dpdkDriver) {
        delete dpdkDriver;
    }
#endif
}

/**
 * This method is invoked only on servers; it creates transport(s) that will be
 * used to receive RPC requests.  These transports can also be used for outgoing
 * RPC requests, and additional transports for outgoing requests will be
 * created on-demand by #getSession.
 *
 * \param localServiceLocator
 *      Specifies one or more locators that clients can use to send requests to
 *      this server.
 */
void
TransportManager::initialize(const char* localServiceLocator)
{
    isServer = true;
    Dispatch::Lock lock(context->dispatch);
    std::vector<ServiceLocator> locators =
            ServiceLocator::parseServiceLocators(localServiceLocator);

    if (locators.empty()) {
        throw Exception(HERE,
            "Servers must listen on at least one service locator, but "
            "none was provided");
    }

    uint32_t numListeningTransports = 0;
    foreach (auto& locator, locators) {
        for (uint32_t i = 0; i < transportFactories.size(); i++) {
            TransportFactory* factory = transportFactories[i];
            if (factory->supports(locator.getProtocol().c_str())) {
                // The transport supports a protocol that we can receive
                // requests on.
                Transport *transport = factory->createTransport(context,
                                                                &locator);
                for (uint32_t j = 0; j < registeredBases.size(); j++) {
                    transport->registerMemory(registeredBases[j],
                                              registeredSizes[j]);
                }
                if (transports[i] == NULL) {
                    transports[i] = transport;
                } else {
                    // If we get here, it means we've already created at
                    // least one transport for this factory.
                    transports.push_back(transport);
                }
                // Ask the transport for its service locator. This might be
                // more specific than "locator", as the transport may have
                // added information.
                string listeningLocator = transport->getServiceLocator();
                if (listeningLocator.empty()) {
                    throw Exception(HERE,
                        format("Listening transport has empty locator. "
                               "Was initialized with '%s'",
                               locator.getOriginalString().c_str()));
                }
                if (listeningLocators.size() != 0)
                    listeningLocators += ";";
                listeningLocators += listeningLocator;
                ++numListeningTransports;
                break;
            }
        }
    }
    if (numListeningTransports == 0) {
        dumpTransportFactories();
        throw Exception(HERE, format(
            "Servers must listen on at least one service locator, but no "
            "possible transports were found for '%s'", localServiceLocator));
    }
}

/**
 * Remove a session from the session cache, if it is present.
 *
 * \param serviceLocator
 *      Service locator for which any existing session should be flushed (was
 *      passed to an earlier call to getSession).
 */
void
TransportManager::flushSession(const string& serviceLocator)
{
    // If we're running on a server (i.e., multithreaded) must exclude
    // other threads.
    Tub<std::lock_guard<SpinLock>> lock;
    if (isServer) {
        lock.construct(mutex);
    }

    TEST_LOG("flushing session for %s", serviceLocator.c_str());
    auto it = sessionCache.find(serviceLocator);
    if (it != sessionCache.end())
        sessionCache.erase(it);
}

/**
 * Get a session on which to send RPC requests to a service.  This method
 * keeps a cache of sessions and will reuse existing sessions whenever
 * possible. If necessary, the method also instantiates new transports
 * based on the service locator.
 *
 * \param serviceLocator
 *      Desired service.
 *
 * \return
 *      A session corresponding to serviceLocator. If a session could not
 *      be opened for serviceLocator then the error gets logged and a
 *      FailSession is returned.
 *
 * \throw NoSuchKeyException
 *      A transport supporting one of the protocols claims a service locator
 *      option is missing.
 * \throw BadValueException
 *      A transport supporting one of the protocols claims a service locator
 *      option is malformed.
 */
Transport::SessionRef
TransportManager::getSession(const string& serviceLocator)
{
    // If we're running on a server (i.e., multithreaded) must exclude
    // other threads.
    Tub<SpinLock::Guard> lock;
    if (isServer) {
        lock.construct(mutex);
    }

    // First check to see if we have already opened a session for the
    // locator; this should almost always be true.
    SessionCache::iterator it = sessionCache.find(serviceLocator);
    if (it != sessionCache.end()) {
        return it->second;
    }

    CycleCounter<RawMetric> counter;

    // Session was not found in the cache, so create a new one and add
    // it to the cache.
    Transport::SessionRef session(openSessionInternal(serviceLocator));
    sessionCache.insert({serviceLocator, session});
    return session;
}

/**
 * Return a ServiceLocator string corresponding to the listening
 * ServiceLocators.
 * \return
 *      A semicolon-delimited, ServiceLocator string containing all
 *      ServiceLocators' strings that are listening to RPCs.
 */
string
TransportManager::getListeningLocatorsString()
{
    return listeningLocators;
}

/**
 * Given a service locator, open a new session connected to that service
 * locator.  If necessary, this method also instantiates a new transport
 * based on the service locator.  This method accesses no shared data, so
 * it is thread-safe.
 *
 * \param serviceLocator
 *      Desired service.
 *
 * \return
 *      A reference to the new session. If a session could not be opened,
 *      an error message is logged and a FailSession is returned.
 */
Transport::SessionRef
TransportManager::openSession(const string& serviceLocator)
{
    // If we're running on a server (i.e., multithreaded) must exclude
    // other threads.
    Tub<std::lock_guard<SpinLock>> lock;
    if (isServer) {
        lock.construct(mutex);
    }
    return openSessionInternal(serviceLocator);
}

/**
 * This method does all the real work of openSession; it is separate so
 * that it can be used by other methods such as getSession. The caller must
 * have acquired the TransportManager lock.
 *
 * \param serviceLocator
 *      Desired service.
 *
 * \return
 *      A reference to the new session. If a session could not be opened,
 *      an error message is logged and a FailSession is returned.
 */
Transport::SessionRef
TransportManager::openSessionInternal(const string& serviceLocator)
{
    CycleCounter<RawMetric> _(&metrics->transport.sessionOpenTicks);
    // Collects error messages from all the transports that tried to
    // open a session from this locator.
    string messages;

    // Iterate over all of the sub-locators, looking for a transport that
    // can handle its protocol.
    vector<ServiceLocator> locators =
            ServiceLocator::parseServiceLocators(serviceLocator);
    foreach (ServiceLocator& locator, locators) {
        for (uint32_t i = 0; i < transportFactories.size(); i++) {
            TransportFactory* factory = transportFactories[i];
            if (!factory->supports(locator.getProtocol().c_str())) {
                continue;
            }

            if (transports[i] == NULL) {
                // Try to create a new transport via this factory.
                // It's OK if that doesn't work (e.g. the particular
                // transport may depend on physical devices that don't
                // exist on this machine).
                try {
                    Dispatch::Lock lock(context->dispatch);
                    transports[i] = factory->createTransport(context, NULL);
                    for (uint32_t j = 0; j < registeredBases.size(); j++) {
                        transports[i]->registerMemory(registeredBases[j],
                                                      registeredSizes[j]);
                    }
                } catch (TransportException &e) {
                    continue;
                }
            }

            try {
                Transport::SessionRef session = transports[i]->getSession(
                        &locator, sessionTimeoutMs);
                if (isServer) {
                    return new WorkerSession(context, session);
                }
                return session;
            } catch (TransportException& e) {
                // Save error information in case none of the locators works.
                if (locators.size() == 1) {
                    messages = e.message;
                } else {
                    if (!messages.empty()) {
                        messages += ", ";
                    }
                    messages += locator.getOriginalString().c_str();
                    messages += ": ";
                    messages += e.message;
                }
            }
        }
    }

    if (messages.empty()) {
        LOG(WARNING, "No supported transport found for locator %s",
                serviceLocator.c_str());
    } else {
        LOG(WARNING, "Couldn't open session for locator %s (%s)",
                serviceLocator.c_str(), messages.c_str());
    }
    return FailSession::get();
}

/**
 * See #Transport::registerMemory.
 */
void
TransportManager::registerMemory(void* base, size_t bytes)
{
    Dispatch::Lock lock(context->dispatch);
    foreach (auto transport, transports) {
        if (transport != NULL)
            transport->registerMemory(base, bytes);
    }
    registeredBases.push_back(base);
    registeredSizes.push_back(bytes);
}

/**
 * Use a particular timeout value for all new transports created from now on.
 *
 * \param timeoutMs
 *      Timeout period (in ms) to pass to transports.
 */
void TransportManager::setSessionTimeout(uint32_t timeoutMs)
{
    this->sessionTimeoutMs = timeoutMs;
}

/**
 * Return current timeout value (ms) for all server port created from now on.
 *
 */
uint32_t TransportManager::getSessionTimeout() const
{
    return sessionTimeoutMs;
}

/**
 * Calls dumpStats() on all existing transports.
 */
void
TransportManager::dumpStats()
{
    Dispatch::Lock lock(context->dispatch);
    foreach (auto transport, transports) {
        if (transport != NULL)
            transport->dumpStats();
    }
}

/**
 * Logs the list of transport factories and the protocols they support.
 */
void
TransportManager::dumpTransportFactories()
{
    Dispatch::Lock lock(context->dispatch);
    string list;
    string separator = "";
    foreach (auto factory, transportFactories) {
        int count = 0;
        foreach (const char* protocol, factory->getProtocols()) {
            if (count == 0) {
                list.append(separator);
            } else if (count == 1) {
                list.append(" (");
            } else {
                list.append(", ");
            }
            list.append(protocol);
            count++;
        }
        if (count > 1) {
            list.append(")");
        }
        separator = ", ";
    }
    LOG(NOTICE, "Known transports: %s", list.c_str());
}

} // namespace RAMCloud
