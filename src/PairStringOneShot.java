import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairStringOneShot implements WritableComparable<PairStringOneShot>  {
	
	private String first;
	
	private String second;
	
	
	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}

	public PairStringOneShot() {
		super();
	}

	public PairStringOneShot(String first, String second) {
		super();
		this.first = first;
		this.second = second;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		Text t = new Text();
		t.readFields(arg0);
		this.first = t.toString();
		t.readFields(arg0);
		this.second = t.toString();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		Text t = new Text(this.first);
		t.write(arg0);
		t = new Text(this.second);
		t.write(arg0);
	}

	@Override
	public int compareTo(PairStringOneShot arg0) {
		int res = this.first.compareTo(arg0.first);
		//if (res == 0) return this.second.compareTo(arg0.second);
		return res;
	}

}
