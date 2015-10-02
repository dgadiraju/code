package nyse.comparators;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import nyse.keyvalues.LongPair;

public class FirstFieldLongPairGroupingComparator extends WritableComparator {
	private static final LongWritable.Comparator LONGWRITABLE_COMPARATOR = new LongWritable.Comparator();
	
	public FirstFieldLongPairGroupingComparator() {
		super(LongPair.class);
	}
	
	public int compare(byte[] firstObjBytes, int firstFieldByteAddr, int firstObjLen,
			byte[]secondObjBytes, int secondFieldByteAddr, int secondObjLen) {
		try {
			int firstFieldLen = 
					WritableUtils.decodeVIntSize(firstObjBytes[firstFieldByteAddr]) 
					+ readVInt(firstObjBytes, firstFieldByteAddr);
			int secondFieldLen = 
					WritableUtils.decodeVIntSize(secondObjBytes[secondFieldByteAddr]) 
					+ readVInt(secondObjBytes, secondFieldByteAddr);
			int cmp = LONGWRITABLE_COMPARATOR.compare(firstObjBytes, firstFieldByteAddr, 
					firstFieldLen, secondObjBytes, 
					secondFieldByteAddr, secondFieldLen);
			
			return cmp;
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 0;
	}
}
