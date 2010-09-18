package net.spy.memcached.vbucket;

import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpMessage;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * @author alexander.sokolovsky.a@gmail.com
 */
public class VBucketHttpChunkAggregator extends SimpleChannelUpstreamHandler {
    /**
     * Content length that can not be exceeded.
     */
    private final int maxContentLength;
    /**
     * Message buffer
     */
    private volatile HttpMessage currentMessage;

    /**
     * Creates a new instance.
     *
     * @param maxContentLength
     *        the maximum length of the aggregated content.
     *        If the length of the aggregated content exceeds this value,
     *        a {@link org.jboss.netty.handler.codec.frame.TooLongFrameException} will be raised.
     */
    public VBucketHttpChunkAggregator(int maxContentLength) {
        if (maxContentLength <= 0) {
            throw new IllegalArgumentException(
                    "maxContentLength must be a positive integer: " +
                    maxContentLength);
        }
        this.maxContentLength = maxContentLength;
    }


}
