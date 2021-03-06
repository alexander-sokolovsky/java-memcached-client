package net.spy.memcached.vbucket;

import java.util.Observer;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.net.URI;
import java.net.InetSocketAddress;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.Authenticator;
import java.net.URL;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;

import com.northscale.jvbucket.Config;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.vbucket.config.Bucket;
import net.spy.memcached.vbucket.config.Pool;
import net.spy.memcached.vbucket.config.ConfigurationParserJSON;
import net.spy.memcached.vbucket.config.ConfigurationParser;

import javax.naming.ConfigurationException;

/**
 * @author alexander.sokolovsky.a@gmail.com
 */
public class ConfigurationProviderHTTP implements ConfigurationProvider {
    private static final Logger logger = Logger.getLogger(ConfigurationProviderHTTP.class.getName());
    private static final String DEFAULT_POOL_NAME = "default";
    private static final String ANONYMOUS_AUTH_BUCKET = "default";
    /**
     * The specification version which this client meets.  This will be included
     * in requests to the server.
     */
    public static final String CLIENT_SPEC_VER = "1.0";
    private List<URI> baseList;
    private String restUsr;
    private String restPwd;
    private URI loadedBaseUri;
    // map of <bucketname, bucket> currently loaded
    private Map<String, Bucket> buckets = new ConcurrentHashMap<String, Bucket>();

    // map of <poolname, pool> currently loaded
    //private Map<String, Pool> pools = new ConcurrentHashMap<String, Pool>();
    private ConfigurationParser configurationParser = new ConfigurationParserJSON();
    private Map<String, BucketMonitor> monitors = new HashMap<String, BucketMonitor>();

    public ConfigurationProviderHTTP(List<URI> baseList) throws IOException {
        this(baseList, null, null);
    }

    public ConfigurationProviderHTTP(List<URI> baseList, String restUsr, String restPwd) throws IOException {
        this.baseList = baseList;
        this.restUsr = restUsr;
        this.restPwd = restPwd;
    }

    public Bucket getBucketConfiguration(final String bucketname) throws ConfigurationException {
        if (bucketname == null || bucketname.isEmpty()) {
            throw new IllegalArgumentException("Bucket name can not be blank.");
        }
        Bucket bucket = this.buckets.get(bucketname);
        if (bucket == null) {
            readPools(bucketname);
        }
        return this.buckets.get(bucketname);
    }

    private void readPools(final String bucketToFind) throws ConfigurationException {
        for (URI baseUri : baseList) {
            try {
                // get and parse the response from the current base uri
                URLConnection baseConnection = urlConnBuilder(null, baseUri);
                String base = readToString(baseConnection);
                if ("".equals(base)) {
                    logger.log(Level.WARNING, "Provided URI " + baseUri + " has an empty response...skipping");
                    continue;
                }
                Map<String, Pool> pools = this.configurationParser.parseBase(base);

                // check for the default pool name
                if (!pools.containsKey(DEFAULT_POOL_NAME)) {
                    logger.log(Level.WARNING, "Provided URI " + baseUri + " has no default pool...skipping");
                    continue;
                }
                // load pools
                for (Pool pool : pools.values()) {
                    URLConnection poolConnection = urlConnBuilder(baseUri, pool.getUri());
                    String sPool = readToString(poolConnection);
                    configurationParser.loadPool(pool, sPool);
                    URLConnection poolBucketsConnection = urlConnBuilder(baseUri, pool.getBucketsUri());
                    String sBuckets = readToString(poolBucketsConnection);
                    Map<String, Bucket> buckets = configurationParser.parseBuckets(sBuckets);
                    pool.getBuckets().putAll(buckets);

                }
                // did we found our bucket?
                boolean bucketFound = false;
                for (Pool pool : pools.values()) {
                    if (pool.getBuckets().containsKey(bucketToFind)) {
                        bucketFound = true;
                    }
                }
                if (bucketFound) {
                    // set values
                    //this.pools.clear();
                    //this.pools.putAll(pools);
                    for (Pool pool : pools.values()) {
                        for (Map.Entry<String, Bucket> bucketEntry : pool.getBuckets().entrySet()) {
                            this.buckets.put(bucketEntry.getKey(), bucketEntry.getValue());
                        }
                    }
                    this.loadedBaseUri = baseUri;
                    return;
                }
            } catch (ParseException e) {
                logger.log(Level.WARNING, "Provided URI " + baseUri + " has an unparsable response...skipping", e);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Connection problems with URI " + baseUri + " ...skipping", e);
            }
            throw new ConfigurationException("Configuration for bucket " + bucketToFind + " was not found.");
        }
    }

    public List<InetSocketAddress> getServerList(final String bucketname) throws ConfigurationException {
        Bucket bucket = getBucketConfiguration(bucketname);
        List<String> servers = bucket.getVbuckets().getServers();
        StringBuilder serversString = new StringBuilder();
        for (String server : servers) {
            serversString.append(server).append(" ");
        }
        return AddrUtil.getAddresses(serversString.toString());
    }

    public void subscribe(final String bucketName, final Reconfigurable rec) throws ConfigurationException {

        Bucket bucket = getBucketConfiguration(bucketName);

        ReconfigurableObserver obs = new ReconfigurableObserver(rec);
        BucketMonitor monitor = this.monitors.get(bucketName);
        if (monitor == null) {
            URI streamingURI = bucket.getStreamingURI();
            monitor = new BucketMonitor(this.loadedBaseUri.resolve(streamingURI), bucketName, this.restUsr, this.restPwd, configurationParser);
            this.monitors.put(bucketName, monitor);
            monitor.addObserver(obs);
            monitor.startMonitor();
        } else {
            monitor.addObserver(obs);
        }
    }

    public void unsubscribe(String vbucketName, Reconfigurable rec) {
        BucketMonitor monitor = this.monitors.get(vbucketName);
        if (monitor != null) {
            monitor.deleteObserver(new ReconfigurableObserver(rec));
        }
    }

    public Config getLatestConfig(String bucketname) throws ConfigurationException {
        Bucket bucket = getBucketConfiguration(bucketname);
        return bucket.getVbuckets();
    }

    public String getAnonymousAuthBucket() {
        return ANONYMOUS_AUTH_BUCKET;
    }

    public void shutdown() {
        for (BucketMonitor monitor : this.monitors.values()) {
            monitor.shutdown();
        }
    }

    /**
     * Create a URL which has the appropriate headers to interact with the
     * service.  Most exception handling is up to the caller.
     *
     * @param resource the URI either absolute or relative to the base for this ClientManager
     * @return
     * @throws java.io.IOException
     */
    private URLConnection urlConnBuilder(URI base, URI resource) throws IOException {
        if (!resource.isAbsolute() && base != null) {
            resource = base.resolve(resource);
        }
        if (restUsr != null) {
            Authenticator.setDefault(new PoolAuthenticator(this.restUsr, this.restPwd));
        } else {
            Authenticator.setDefault(null);
        }
        URL specURL = resource.toURL();
        URLConnection connection = specURL.openConnection();
        connection.setRequestProperty("Accept", "application/com.northscale.store+json");
        connection.setRequestProperty("user-agent", "NorthScale Java Client from_git_stauts");
        connection.setRequestProperty("X-memcachekv-Store-Client-Specification-Version", CLIENT_SPEC_VER);

        return connection;

    }

    private String readToString(URLConnection connection) throws IOException {
        InputStream inStream = connection.getInputStream();
        if (connection instanceof java.net.HttpURLConnection) {
            HttpURLConnection httpConnection = (HttpURLConnection) connection;
            if (httpConnection.getResponseCode() == 403) {
                throw new IOException("Service does not accept the authentication credentials: "
                        + httpConnection.getResponseCode() + httpConnection.getResponseMessage());
            } else if (httpConnection.getResponseCode() >= 400) {
                throw new IOException("Service responded with a failure code: "
                        + httpConnection.getResponseCode() + httpConnection.getResponseMessage());
            }
        } else {
            throw new IOException("Unexpected URI type encountered");
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
        String str;
        StringBuffer buffer = new StringBuffer();
        while ((str = reader.readLine()) != null) {
            buffer.append(str);
        }
        reader.close();
        return buffer.toString();
    }

}
