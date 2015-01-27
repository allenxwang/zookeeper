package org.apache.zookeeper.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

/**
 * A HostProvider that refreshes cached DNS lookup result every 30 seconds
 */
public class StaticHostProviderWithDNSRefresh implements HostProvider {
    private static final Logger LOG = LoggerFactory
            .getLogger(StaticHostProvider.class);

    private volatile List<InetSocketAddress> cachedServerAddresses = new ArrayList<InetSocketAddress>(
            5);

    private int lastIndex = -1;
    private int currentIndex = -1;

    private final List<InetSocketAddress> unresolvedHosts;

    private final HostProvider staticProvider;

    private final boolean shouldRefreshDNS;

    private final ScheduledExecutorService executorService;

    /**
     * Constructs a SimpleHostSet.
     *
     * @param serverAddresses
     *            possibly unresolved ZooKeeper server addresses
     * @throws java.net.UnknownHostException
     * @throws IllegalArgumentException
     *             if serverAddresses is empty or resolves to an empty list
     */
    public StaticHostProviderWithDNSRefresh(Collection<InetSocketAddress> serverAddresses)
            throws UnknownHostException {
        // check if the raw addresses are formed from host names. If any
        // of the host is IP address, we will fallback to StaticHostProvider
        shouldRefreshDNS = checkUnresolvedAddresses(serverAddresses);
        if (shouldRefreshDNS) {
            unresolvedHosts = new ArrayList<InetSocketAddress>(serverAddresses);
            Collections.shuffle(unresolvedHosts);
            staticProvider = null;
            executorService = Executors.newScheduledThreadPool(1);
            refreshDNS();
            LOG.info("Initial zookeeper server addresses: {}", cachedServerAddresses);
            if (serverAddresses.isEmpty()) {
                throw new IllegalArgumentException(
                        "A HostProvider may not be empty!");
            }
            executorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    LOG.debug("DNS refresh started");
                    refreshDNS();
                }
            }, 30, 30, TimeUnit.SECONDS);
        } else {
            LOG.warn("Will skip DNS refresh as IP addresses exist in initial server addresses: {}", serverAddresses);
            unresolvedHosts = null;
            staticProvider = new StaticHostProvider(serverAddresses);
            executorService = null;
        }
    }

    private boolean checkUnresolvedAddresses(Collection<InetSocketAddress> serverAddresses) throws UnknownHostException {
        boolean hasHostName = true;
        for (InetSocketAddress address : serverAddresses) {
            InetAddress ia = address.getAddress();
            InetAddress resolvedAddresses[] = InetAddress.getAllByName((ia!=null) ? ia.getHostAddress():
                    address.getHostName());
            for (InetAddress resolvedAddress : resolvedAddresses) {
                // If hostName is null but the address is not, we can tell that
                // the hostName is an literal IP address. Then we can set the host string as the hostname
                // safely to avoid reverse DNS lookup.
                // As far as i know, the only way to check if the hostName is null is use toString().
                // Both the two implementations of InetAddress are final class, so we can trust the return value of
                // the toString() method.
                if (resolvedAddress.toString().startsWith("/")
                        && resolvedAddress.getAddress() != null) {
                    hasHostName = false;
                }
            }
        }
        return hasHostName;
    }

    private void refreshDNS() {
        List<InetSocketAddress> newList = new ArrayList<InetSocketAddress>(size());
        for (InetSocketAddress unresolved: unresolvedHosts) {
            try {
                InetAddress[] resolvedAddresses = InetAddress.getAllByName(unresolved.getHostName());
                for (InetAddress resolvedAddress: resolvedAddresses) {
                    newList.add(new InetSocketAddress(resolvedAddress.getHostAddress(), unresolved.getPort()));
                }
            } catch (UnknownHostException e) {
                LOG.warn("Unable to resolve host", e);
            }
        }
        if (newList.size() > 0 && !newList.equals(cachedServerAddresses)) {
            cachedServerAddresses = newList;
            LOG.info("Setting new list of zookeeper server addresses: {}", cachedServerAddresses);
        }
    }

    @Override
    public int size() {
        if (!shouldRefreshDNS) {
            return staticProvider.size();
        }
        return cachedServerAddresses.size();
    }

    @Override
    public InetSocketAddress next(long spinDelay) {
        if (!shouldRefreshDNS) {
            return staticProvider.next(spinDelay);
        }
        List<InetSocketAddress> addresses = cachedServerAddresses;
        // The following code is mostly copied from StaticHostProvider
        // No thread safety is guaranteed as this method is only called
        // from single thread in ClientCnxn.SendThread
        currentIndex = ++currentIndex % addresses.size();
        // since address list may change, lastIndex may be out of range
        // of the current list. lastIndex % addresses.size() will ensure
        // spinDelay will apply at some point in that case.
        if (currentIndex == (lastIndex % addresses.size()) && spinDelay > 0) {
            try {
                Thread.sleep(spinDelay);
            } catch (InterruptedException e) {
                LOG.warn("Unexpected exception", e);
            }
        } else if (lastIndex == -1) {
            // We don't want to sleep on the first ever connect attempt.
            lastIndex = 0;
        }
        return addresses.get(currentIndex);
    }

    @Override
    public void onConnected() {
        if (!shouldRefreshDNS) {
            staticProvider.onConnected();
        } else {
            // The following code is copied from StaticHostProvider
            // No thread safety is guaranteed as this method is only called
            // from single thread in ClientCnxn.SendThread
            lastIndex = currentIndex;
        }
    }

}
