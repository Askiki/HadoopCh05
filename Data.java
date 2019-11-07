package com.study;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Data implements Writable {
    private String time;
    private String number;
    private String name;
    private int rank;
    private int order;
    private String wed;
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.time);
        dataOutput.writeUTF(this.number);
        dataOutput.writeUTF(this.name);
        dataOutput.writeInt(this.rank);
        dataOutput.writeInt(this.order);
        dataOutput.writeUTF(this.wed);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.time = dataInput.readUTF();
        this.number = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.rank = dataInput.readInt();
        this.order = dataInput.readInt();
        this.wed = dataInput.readUTF();
    }
    @Override
    public String toString() {
        return this.getTime()+"  "+this.getNumber()+"  "+this.getName()+"  "+this.getRank()+"  "+this.getOrder()+"  "+this.getWed();
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public String getWed() {
        return wed;
    }

    public void setWed(String wed) {
        this.wed = wed;
    }
}
