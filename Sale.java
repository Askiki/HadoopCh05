package com.study;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Sale implements Writable {
    private int prod_id;        
    private String time;         
    private int channel_id;       
    private int promo_id;         
    private int quantity_sold;    
    private float amount_sold;
    private int cust_id;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       
        dataOutput.writeInt(this.prod_id);
        dataOutput.writeInt(this.cust_id);
        dataOutput.writeUTF(this.time);
        dataOutput.writeInt(this.channel_id);
        dataOutput.writeInt(this.promo_id);
        dataOutput.writeInt(this.quantity_sold);
        dataOutput.writeFloat(this.amount_sold);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      
        this.prod_id = dataInput.readInt();
        this.cust_id = dataInput.readInt();
        this.time = dataInput.readUTF();
        this.channel_id = dataInput.readInt();
        this.promo_id = dataInput.readInt();
        this.quantity_sold = dataInput.readInt();
        this.amount_sold = dataInput.readFloat();
    }

    public int getProd_id() {
        return prod_id;
    }

    public void setProd_id(int prod_id) {
        this.prod_id = prod_id;
    }

    public int getCust_id() {
        return cust_id;
    }

    public void setCust_id(int cust_id) {
        this.cust_id = cust_id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getChannel_id() {
        return channel_id;
    }

    public void setChannel_id(int channel_id) {
        this.channel_id = channel_id;
    }

    public int getPromo_id() {
        return promo_id;
    }

    public void setPromo_id(int promo_id) {
        this.promo_id = promo_id;
    }

    public int getQuantity_sold() {
        return quantity_sold;
    }

    public void setQuantity_sold(int quantity_sold) {
        this.quantity_sold = quantity_sold;
    }

    public float getAmount_sold() {
        return amount_sold;
    }

    public void setAmount_sold(float amount_sold) {
        this.amount_sold = amount_sold;
    }
}
