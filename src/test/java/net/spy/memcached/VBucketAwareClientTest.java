package net.spy.memcached;

import junit.framework.TestCase;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;

import net.spy.memcached.internal.BulkFuture;

/**
 * @author alexander.sokolovsky.a@gmail.com
 */
public class VBucketAwareClientTest extends TestCase {
    public void testInstantiate() throws Exception {
        URI baseUri = new URI("http://localhost:8080/pools");
        MemcachedClient mc = new MemcachedClient(Arrays.asList(baseUri), "Administrator", "Administrator", "password", true);
        assertNotNull(mc);
        Future<Object> getH12 = mc.asyncGet("h12");
        Object valueH12 = getH12.get();
        assertNotNull(valueH12);
        assertNotNull(valueH12);
    }

}
