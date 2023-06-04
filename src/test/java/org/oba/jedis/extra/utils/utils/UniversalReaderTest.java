package org.oba.jedis.extra.utils.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class UniversalReaderTest {

    @Test
    public void testWithResource() {
        String result1 = new UniversalReader().
                withResoruce("example1.txt").read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example1"));
    }

    @Test
    public void testWithFile() {
        String result1 = new UniversalReader().
                withFile("./src/test/resources/example1.txt").read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example1"));
    }

    @Test
    public void testWithValue() {
        String result1 = new UniversalReader().
                withValue("example4.txt").read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example4"));
    }


    @Test
    public void testWithFileAndResource1() {
        String result1 = new UniversalReader().
                withFile("./src/test/resources/example3.txt").
                withResoruce("example2.txt").
                read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example2"));
    }

    @Test
    public void testWithAndResourceAndFile1() {
        String result1 = new UniversalReader().
                withResoruce("example3.txt").
                withFile("./src/test/resources/example2.txt").
                read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example2"));
    }


    @Test
    public void testWithFileAndResource2() {
        String result1 = new UniversalReader().
                withFile("./src/test/resources/example1.txt").
                withResoruce("example2.txt").
                read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example1"));
    }

    @Test
    public void testWithAndResourceAndFile2() {
        String result1 = new UniversalReader().
                withResoruce("example1.txt").
                withFile("./src/test/resources/example2.txt").
                read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example1"));
    }

    @Test
    public void testWithFileAndResourceAndValue1() {
        String result1 = new UniversalReader().
                withFile("./src/test/resources/example3.txt").
                withResoruce("example4.txt").
                withValue("hello").
                read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("hello"));
    }

    @Test
    public void testWithFileAndResourceAndValue2() {
        String result1 = new UniversalReader().
                withFile("./src/test/resources/example3.txt").
                withResoruce("example2.txt").
                withValue("hello").
                read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example2"));
    }

    @Test
    public void testWithFileAndResourceAndValue3() {
        String result1 = new UniversalReader().
                withFile("./src/test/resources/example1.txt").
                withResoruce("example2.txt").
                withValue("hello").
                read();
        result1 = Arrays.stream(result1.split("\n")).
                collect(Collectors.joining(""));
        assertTrue(result1.contains("example1"));
    }

}
