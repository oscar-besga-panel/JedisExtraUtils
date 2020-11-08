package org.obapanel.jedis.mapper;

import java.util.Objects;

public class PojoDataTest {

    public static PojoDataTest pojoDataTestExample1 = new PojoDataTest("name1",true,'1',(byte)1,(short)1,1,1,(float)1.1,1.1);
    public static PojoDataTest pojoDataTestExample2 = new PojoDataTest("name2",false,'2',(byte)2,(short)2,2,2,(float)2.2,2.2);
    public static PojoDataTest pojoDataTestExample3 = new PojoDataTest("name3",true,'3',(byte)3, (short)3,3,3,(float)3.3,3.3);


    private String name;
    private boolean bool;
    private char character;
    private byte byteNum;
    private short shortNum;
    private int intNum;
    private long longNum;
    private float floatNum;
    private double doubleNum;

    public PojoDataTest(){}

    public PojoDataTest(String name, boolean bool, char character, byte byteNum, short shortNum, int intNum, long longNum, float floatNum, double doubleNum) {
        this.name = name;
        this.bool = bool;
        this.character = character;
        this.byteNum = byteNum;
        this.shortNum = shortNum;
        this.intNum = intNum;
        this.longNum = longNum;
        this.floatNum = floatNum;
        this.doubleNum = doubleNum;
    }

    public PojoDataTest(PojoDataTest other) {
        this.name = other.name;
        this.bool = other.bool;
        this.character = other.character;
        this.byteNum = other.byteNum;
        this.shortNum = other.shortNum;
        this.intNum = other.intNum;
        this.longNum = other.longNum;
        this.floatNum = other.floatNum;
        this.doubleNum = other.doubleNum;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIntNum() {
        return intNum;
    }

    public void setIntNum(int intNum) {
        this.intNum = intNum;
    }

    public long getWeigth() {
        return longNum;
    }

    public void setWeigth(long weigth) {
        this.longNum = weigth;
    }

    public double getDoubleNum() {
        return doubleNum;
    }

    public void setDoubleNum(double doubleNum) {
        this.doubleNum = doubleNum;
    }

    public boolean isBool() {
        return bool;
    }

    public void setBool(boolean bool) {
        this.bool = bool;
    }

    public char getCharacter() {
        return character;
    }

    public void setCharacter(char character) {
        this.character = character;
    }

    public byte getByteNum() {
        return byteNum;
    }

    public void setByteNum(byte byteNum) {
        this.byteNum = byteNum;
    }

    public short getShortNum() {
        return shortNum;
    }

    public void setShortNum(short shortNum) {
        this.shortNum = shortNum;
    }

    public long getLongNum() {
        return longNum;
    }

    public void setLongNum(long longNum) {
        this.longNum = longNum;
    }

    public float getFloatNum() {
        return floatNum;
    }

    public void setFloatNum(float floatNum) {
        this.floatNum = floatNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PojoDataTest)) return false;
        PojoDataTest that = (PojoDataTest) o;
        return bool == that.bool &&
                character == that.character &&
                byteNum == that.byteNum &&
                shortNum == that.shortNum &&
                intNum == that.intNum &&
                longNum == that.longNum &&
                Float.compare(that.floatNum, floatNum) == 0 &&
                Double.compare(that.doubleNum, doubleNum) == 0 &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, bool, character, byteNum, shortNum, intNum, longNum, floatNum, doubleNum);
    }

    @Override
    public String toString() {
        return "PojoDataTest{" +
                "name='" + name + '\'' +
                ", bool=" + bool +
                ", character=" + character +
                ", byteNum=" + byteNum +
                ", shortNum=" + shortNum +
                ", intNum=" + intNum +
                ", longNum=" + longNum +
                ", floatNum=" + floatNum +
                ", doubleNum=" + doubleNum +
                '}';
    }
}
