package net.spy.memcached.vbucket;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import net.spy.memcached.vbucket.config.Port;
import net.spy.memcached.vbucket.config.Bucket;
import net.spy.memcached.vbucket.config.Node;
import net.spy.memcached.vbucket.config.Status;

import javax.naming.ConfigurationException;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicReference;
import java.net.URI;
import java.net.InetSocketAddress;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import com.northscale.jvbucket.Config;


/**
 * @author alexander.sokolovsky.a@gmail.com
 */
public class ClientManager implements Reconfigurable {
    private final AtomicReference<MemcachedClient> clientAR = new AtomicReference<MemcachedClient>();
    private final AtomicReference<MemcachedClient> vbucketAwareClientAR = new AtomicReference<MemcachedClient>();
    private ConnectionFactoryBuilder cfb;
    private String username;
    private String userpassword;
    private String bucketname;
    private Port preferredPort;
    private ConfigurationProvider configurationProvider;
    private Bucket bucket;

    public ClientManager(List<URI> baseList, String username,
            String userpassword, String bucketname, Port preferredPort , ConfigurationProvider configurationProvider) throws ConfigurationException {
        if (baseList == null) {
            throw new IllegalArgumentException("Base List can not be null");
        }
        if (configurationProvider == null) {
            throw new IllegalArgumentException("Configuration provider can not be null");
        }
        if ((!bucketname.equals(username)) && (!bucketname.equals(configurationProvider.getAnonymousAuthBucket()))) {
            throw new IllegalArgumentException("Bucket specified must be the user's bucket or the anonymous bucket");
        }
        this.username = username;
        this.userpassword = userpassword;
        this.bucketname = bucketname;
        this.preferredPort = preferredPort;
        this.configurationProvider = configurationProvider;
        checkBaseList(baseList);
        this.bucket = configurationProvider.getBucketConfiguration(this.bucketname);
        configurationProvider.subscribe(this.bucketname, this);

    }
    /**
     *
     * Return a net.spy.memcached.MemcachedClient object which has already been
     * configured by the ClientManager to work with the cluster.
     *
     * <p>Applications should use this method to obtain a MemcachedClient and
     * hold on to the reference from that client for a very brief period of
     * time.  An example would be a reference scoped to a short method
     * invocation.
     *
     * @return a net.spy.memcached.MemcachedClient which is using the current
     *         list of servers from the cluster
     * @throws IOException if configuration can not be read or a memcached client can not be instantiated
     */
    public MemcachedClient getClient() throws IOException {
        MemcachedClient client = clientAR.get();
        if (client != null) {
            return client;
        }

        if (this.cfb == null) {
            this.cfb = new ConnectionFactoryBuilder();
        }
        cfb.setFailureMode(FailureMode.Retry)
                .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .setHashAlg(HashAlgorithm.KETAMA_HASH)
                .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT);

        // The server may be "secured" thus requiring REST authentication and
        // yet the user may wish to use the anonymous bucket, which means don't
        // bother with SASL auth
        if (!configurationProvider.getAnonymousAuthBucket().equals(this.bucketname) && this.username != null) {
            AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"},
                    new PlainCallbackHandler(this.username, this.userpassword));
            this.cfb.setAuthDescriptor(ad);
        }

        ConnectionFactory factory = this.cfb.build();
        try {
            List<Node> nodes = this.bucket.getNodes();
            List<InetSocketAddress> serverList = createServerList(nodes);
            client = new MemcachedClient(factory, serverList);
        } catch (IOException ex) {
            Logger.getLogger(ClientManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        assert(client != null);
        if (!clientAR.compareAndSet(null, client)) {
            // Someone else created the client while we worked... reuse the
            // other one.
            client.shutdown();
            client = null;
            return getClient();
        }
        return client;
    }
    public MemcachedClient getVBucketAwareClient() throws IOException {
        MemcachedClient client = vbucketAwareClientAR.get();
        if (client != null) {
            return client;
        }
        Config config = this.bucket.getVbuckets();

        if (this.cfb == null) {
            this.cfb = new ConnectionFactoryBuilder();
        }
        cfb.setFailureMode(FailureMode.Retry)
                .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .setHashAlg(HashAlgorithm.KETAMA_HASH)
                .setLocatorType(ConnectionFactoryBuilder.Locator.VBUCKET)
                .setVBucketConfig(config);

        // The server may be "secured" thus requiring REST authentication and
        // yet the user may wish to use the anonymous bucket, which means don't
        // bother with SASL auth
        if (!configurationProvider.getAnonymousAuthBucket().equals(this.bucketname) && this.username != null) {
            AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"},
                    new PlainCallbackHandler(this.username, this.userpassword));
            this.cfb.setAuthDescriptor(ad);
        }

        ConnectionFactory factory = this.cfb.build();
        try {

            List<InetSocketAddress> serverList = AddrUtil.getAddresses(StringUtils.join(config.getServers(), ','));
            client = new MemcachedClient(factory, serverList);
        } catch (IOException ex) {
            Logger.getLogger(ClientManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        assert(client != null);
        if (!vbucketAwareClientAR.compareAndSet(null, client)) {
            // Someone else created the client while we worked... reuse the
            // other one.
            client.shutdown();
            client = null;
            return getVBucketAwareClient();
        }
        return client;
    }

    private List<InetSocketAddress> createServerList(List<Node> nodes) {
        StringBuilder serverListBuffer = new StringBuilder();
        for (Node node : nodes) {
            if (!Status.unhealthy.equals(node.getStatus())) {
                serverListBuffer
                        .append(AddrUtil.getAddresses(node.getHostname()).get(0).getAddress().getHostAddress())
                        .append(":")
                        .append(node.getPorts().get(this.preferredPort))
                        .append(" ");
            }
        }
        return AddrUtil.getAddresses(serverListBuffer.toString());
    }

    public void reconfigure(Bucket bucket) {
        this.bucket = bucket;
        MemcachedClient client = clientAR.getAndSet(null);
        if (client != null) {
            Logger.getLogger(ClientManager.class.getName()).log(Level.INFO, "List of servers changed.");
            client.shutdown();
        }
        MemcachedClient vBucketAwareclient = vbucketAwareClientAR.getAndSet(null);
        if (vBucketAwareclient != null) {
            Logger.getLogger(ClientManager.class.getName()).log(Level.INFO, "List of servers changed.");
            vBucketAwareclient.shutdown();
        }
    }

    /**
     * @param baseList the baseList to set
     */
    private void checkBaseList(List <URI> baseList) {
        for (URI bu : baseList) {
            if (!bu.isAbsolute()) {
                throw new IllegalArgumentException("The base URI must be absolute");
            }
        }
    }
    
}
