package org.obapanel.jedis.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Enumeration;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;
import java.util.Set;

/**
 * A class that stores properties but doesn't allow to change any value inside
 * Any attempt to change will be met with an exception shot !
 *
 * Use modifiableCopy() method to obtain a modifiable properties object
 */
public final class UnmodifiableProperties extends Properties {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnmodifiableProperties.class);


    public static final Properties EMPTY_UNMODIFIABLE_PROPERTIES = new UnmodifiableProperties();

    public UnmodifiableProperties() {
        super();
    }

    public UnmodifiableProperties(Properties defaults) {
        super(defaults);
    }

    @Override
    public synchronized Object setProperty(String key, String value) {
        throw new UnsupportedOperationException("Unmodifiable properties");
    }

    @Override
    public synchronized void load(Reader reader) throws IOException {
        throw new UnsupportedOperationException("Unmodifiable properties");
    }

    @Override
    public synchronized void load(InputStream inStream) throws IOException {
        throw new UnsupportedOperationException("Unmodifiable properties");
    }

    @Override
    public void save(OutputStream out, String comments) {
        super.save(out, comments);
    }

    @Override
    public void store(Writer writer, String comments) throws IOException {
        super.store(writer, comments);
    }

    @Override
    public void store(OutputStream out, String comments) throws IOException {
        super.store(out, comments);
    }

    @Override
    public synchronized void loadFromXML(InputStream in) throws IOException, InvalidPropertiesFormatException {
        throw new UnsupportedOperationException("Unmodifiable properties");
    }

    @Override
    public void storeToXML(OutputStream os, String comment) throws IOException {
        super.storeToXML(os, comment);
    }

    @Override
    public void storeToXML(OutputStream os, String comment, String encoding) throws IOException {
        super.storeToXML(os, comment, encoding);
    }

    @Override
    public String getProperty(String key) {
        return super.getProperty(key);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return super.getProperty(key, defaultValue);
    }

    @Override
    public Enumeration<?> propertyNames() {
        return super.propertyNames();
    }

    @Override
    public Set<String> stringPropertyNames() {
        return super.stringPropertyNames();
    }

    @Override
    public void list(PrintStream out) {
        super.list(out);
    }

    @Override
    public void list(PrintWriter out) {
        super.list(out);
    }

    public Properties modifiableCopy() {
        return new Properties(this);
    }

}
