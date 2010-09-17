package net.spy.memcached.ops;

/**                                               \
 * @author alexander.sokolovsky.a@gmail.com
 */
public interface VBucketAware {
    void setVBucket(final int vbucket);
}
