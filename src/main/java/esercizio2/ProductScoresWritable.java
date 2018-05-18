package esercizio2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import esercizio1.SummaryWritable;

public class ProductScoresWritable implements WritableComparable<SummaryWritable>{

	private int year;
	private int score;

	public ProductScoresWritable() {

	}

	public ProductScoresWritable(int year, int score) {
		this.year = year;
		this.score = score;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public void readFields(DataInput arg0) throws IOException {
		year = arg0.readInt();
		score = arg0.readInt();
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(year);
		arg0.writeInt(score);
	}

	public int compareTo(SummaryWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return "ProductScoresWritable [year=" + year + ", score=" + score + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + score;
		result = prime * result + year;
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
		ProductScoresWritable other = (ProductScoresWritable) obj;
		if (score != other.score)
			return false;
		if (year != other.year)
			return false;
		return true;
	}

}
