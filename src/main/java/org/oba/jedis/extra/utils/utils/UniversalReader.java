package org.oba.jedis.extra.utils.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Will read the
 */
public class UniversalReader {


    enum Type { RESOURCE, FILE, VALUE}

    private List<Pair<Type,String>> sources = new ArrayList<>();

    public UniversalReader() {

    }

    public UniversalReader withResoruce(String resource) {
        return with(Type.RESOURCE, resource);
    }

    public UniversalReader withFile(String file) {
        return with(Type.FILE, file);
    }

    public UniversalReader withValue(String value) {
        return with(Type.VALUE, value);
    }


    public UniversalReader with(Type type, String file) {
        sources.add(new Pair(type, file));
        return this;
    }

    public String read() {
        String result = null;
        for(Pair<Type,String> source: sources) {
            result = readElement(source);
            if (result != null) {
                break;
            }
        }
        return result;
    }

    String readElement(Pair<Type,String> source) {
        switch (source.getK()) {
            case RESOURCE:
                return readFromResource(source.getV());
            case FILE:
                return readFromFileName(source.getV());
            case VALUE:
                return source.getV();
            default:
                return null;
        }
    }

    String readFromResource(String resource) {
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
            throw new RuntimeException(e);
        }
    }

    String readFromFileName(String fileName) {
        try {
            String result = null;
            File file = new File(fileName);
            if (file.exists() && file.isFile()) {
                result = Files.readAllLines(file.toPath()).stream().
                        collect(Collectors.joining("\n"));
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}