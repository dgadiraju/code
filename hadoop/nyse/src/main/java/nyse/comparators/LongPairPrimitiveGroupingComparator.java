package nyse.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import nyse.keyvalues.LongPairPrimitive;

public class LongPairPrimitiveGroupingComparator extends WritableComparator {
	
	protected LongPairPrimitiveGroupingComparator() {
		super(LongPairPrimitive.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		LongPairPrimitive lp1 = (LongPairPrimitive) a;
		LongPairPrimitive lp2 = (LongPairPrimitive) b;

		return LongPairPrimitive.compare(lp1.getFirst(), lp2.getFirst());
	}

	
}
