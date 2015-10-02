package nyse.keyvalues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LongPairPrimitive implements WritableComparable<LongPairPrimitive> {

	long first;
	long second;

	public LongPairPrimitive() {

	}

	public long getFirst() {
		return first;
	}

	public void setFirst(long first) {
		this.first = first;
	}

	public long getSecond() {
		return second;
	}

	public void setSecond(long second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (first ^ (first >>> 32));
		result = prime * result + (int) (second ^ (second >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LongPairPrimitive other = (LongPairPrimitive) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.first = arg0.readLong();
		this.second = arg0.readLong();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(this.first);
		arg0.writeLong(this.second);
	}

	@Override
	public int compareTo(LongPairPrimitive o) {
		int cmp = compare(first, o.first);
		if(cmp == 0)
			cmp = compare(second, o.second);
		return cmp;
	}
	
	  /**
	   * Convenience method for comparing two ints.
	   */
	  public static int compare(long a, long b) {
	    return (a < b ? -1 : (a == b ? 0 : 1));
	  }

}
