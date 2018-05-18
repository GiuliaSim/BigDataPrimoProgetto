package esercizio1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SummaryWritable implements WritableComparable<SummaryWritable> {

	private String word;
	private int count;
	
	public SummaryWritable() {
	}
	
	public SummaryWritable(String word, int count) {
		this.word = word;
		this.count = count;
	}
	
	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "SummaryWritable [word=" + word + ", count=" + count + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + count;
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		SummaryWritable other = (SummaryWritable) obj;
		if (count != other.count)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	public void readFields(DataInput arg0) throws IOException {
		word = arg0.readUTF();
		count = arg0.readInt();
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(word);
		arg0.writeInt(count);
	}

	public int compareTo(SummaryWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
