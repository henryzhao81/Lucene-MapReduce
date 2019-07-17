package com.openx.audience.xdi.lucence.mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IndexBuilderGroupComparator extends WritableComparator {

	@Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text k1 = (Text) w1;
        Text k2 = (Text) w2;   
//        return k1.getSymbol().compareTo(k2.getSymbol());
		return -1;
    }
}
