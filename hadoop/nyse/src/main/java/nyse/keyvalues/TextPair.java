package nyse.keyvalues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	
	private Text first;
	private Text second;
	
	public TextPair() {
		this.first = new Text();
		this.second = new Text();
	}

	public TextPair(Text first, Text second) {
		super();
		this.first = first;
		this.second = second;
	}

	public TextPair(String first, String second) {
		super();
		this.first = new Text(first);
		this.second = new Text(second);
	}
	
	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.first.readFields(arg0);
		this.second.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.first.write(arg0);
		this.second.write(arg0);
		
	}

	@Override
	public int compareTo(TextPair o) {
		int cmp = this.first.compareTo(o.first);
		if(cmp != 0)
			return cmp;
		return this.second.compareTo(o.second);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
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
		TextPair other = (TextPair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

}
