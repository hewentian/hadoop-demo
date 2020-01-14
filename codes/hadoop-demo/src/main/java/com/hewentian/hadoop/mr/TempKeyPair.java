package com.hewentian.hadoop.mr;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>
 * <b>TempKeyPair</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-03 08:57:09
 * @since JDK 1.8
 */
public class TempKeyPair implements WritableComparable<TempKeyPair> {
    private int year;
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    @Override
    public int compareTo(TempKeyPair o) {
        int res = Integer.compare(this.year, o.getYear());
        if (res != 0) {
            return res;
        }

        return Integer.compare(this.temp, o.getTemp());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.temp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.temp = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "TempKeyPair{" +
                "year=" + year +
                ", temp=" + temp +
                '}';
    }

    @Override
    public int hashCode() {
        return new Integer(year + temp).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }

        if (obj instanceof TempKeyPair == false) {
            return false;
        }

        TempKeyPair other = (TempKeyPair) obj;

        return this.year == other.getYear() && this.temp == other.getTemp();
    }
}
