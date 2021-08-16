package com.atguigu.mapreducetest.joinn;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-22 14:53
 */
public class JoinnBean implements Writable {
    private String id;
    private String pid;
    private Integer amount;
    private String pName;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }


    public JoinnBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pid=in.readUTF();
        amount=in.readInt();
        pName = in.readUTF();
    }

    @Override
    public String toString() {
        return id+"\t"+pName+"\t"+amount;
    }
}
