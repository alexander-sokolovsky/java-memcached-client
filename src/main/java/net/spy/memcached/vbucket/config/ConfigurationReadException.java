package net.spy.memcached.vbucket.config;

/**
 * @author alexander.sokolovsky.a@gmail.com
 */
public class ConfigurationReadException extends Exception {
    public ConfigurationReadException() {
    }

    public ConfigurationReadException(String message) {
        super(message);
    }

    public ConfigurationReadException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationReadException(Throwable cause) {
        super(cause);
    }
}
