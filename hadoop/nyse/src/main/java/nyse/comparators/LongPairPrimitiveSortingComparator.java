package nyse.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import nyse.keyvalues.LongPairPrimitive;

public class LongPairPrimitiveSortingComparator extends WritableComparator {
	
	protected LongPairPrimitiveSortingComparator() {
		super(LongPairPrimitive.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		LongPairPrimitive lp1 = (LongPairPrimitive) a;
		LongPairPrimitive lp2 = (LongPairPrimitive) b;
		int cmp = LongPairPrimitive.compare(lp1.getFirst(), lp2.getFirst());
		
		if(cmp == 0) {
			cmp = -LongPairPrimitive.compare(lp1.getSecond(), lp2.getSecond());
		}
		return cmp;
	}

	
}
