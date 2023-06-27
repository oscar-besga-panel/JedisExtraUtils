package org.oba.jedis.extra.utils.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Will read a file from a resource in classpath, a file or directly from string
 */
public class UniversalReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(UniversalReader.class);

    enum Type { RESOURCE, FILE, VALUE}

    private final List<SimpleEntry> sources = new ArrayList<>();

    public UniversalReader withResoruce(String resource) {
        return with(Type.RESOURCE, resource);
    }

    public UniversalReader withFile(String file) {
        return with(Type.FILE, file);
    }

    public UniversalReader withValue(String value) {
        return with(Type.VALUE, value);
    }


    private UniversalReader with(Type type, String file) {
        sources.add(new SimpleEntry(type.name(), file));
        return this;
    }

    public String read() {
        String result = null;
        for(SimpleEntry source: sources) {
            result = readElement(source);
            if (result != null) {
                break;
            }
        }
        return result;
    }

    String readElement(SimpleEntry source) {
        Type type = Type.valueOf(source.getKey());
        switch (type) {
            case RESOURCE:
                return readFromResource(source.getValue());
            case FILE:
                return readFromFileName(source.getValue());
            case VALUE:
                return readFromValue(source.getValue());
            default:
                return null;
        }
    }

    String readFromResource(String resource) {
        LOGGER.debug("read from {} resource {} ", Type.RESOURCE, resource);
        String result = null;
        InputStream resoureStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            resoureStream = classLoader.getResourceAsStream(resource);
            if (resoureStream != null) {
                inputStreamReader = new InputStreamReader(resoureStream, StandardCharsets.UTF_8);
                bufferedReader = new BufferedReader(inputStreamReader);
                result = bufferedReader.lines().
                        collect(Collectors.joining("\n"));
                bufferedReader.close();
                inputStreamReader.close();
                resoureStream.close();
            }
            return result;
        } catch (IOException e) {
            throw new IllegalStateException("IOException readFromResource", e);
        }
    }

    String readFromFileName(String fileName) {
        LOGGER.debug("read from {} fileName {} ", Type.FILE, fileName);
        try {
            String result = null;
            File file = new File(fileName);
            if (file.exists() && file.isFile()) {
                result = Files.readAllLines(file.toPath()).stream().
                        collect(Collectors.joining("\n"));
            }
            return result;
        } catch (IOException e) {
            throw new IllegalStateException("IOException readFromFileName", e);
        }
    }

    String readFromValue(String value) {
        LOGGER.debug("read from {} value {} ", Type.VALUE, value);
        return value;
    }

}