import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable {

	private Integer intVal;
	private String stringVal;
	
	public StringAndInt(Integer intVal, String stringVal) {
		super();
		this.intVal = intVal;
		this.stringVal = stringVal;
	}
	
	public StringAndInt() {
	}

	@Override
	public int compareTo(StringAndInt o) {
        return this.getIntVal().compareTo(o.getIntVal());
	}
	
	public Integer getIntVal() {
		return intVal;
	}

	public void setIntVal(Integer intVal) {
		this.intVal = intVal;
	}

	public String getStringVal() {
		return stringVal;
	}

	public void setStringVal(String stringVal) {
		this.stringVal = stringVal;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.intVal = arg0.readInt();
		Text t = new Text();
		t.readFields(arg0);
		this.stringVal = t.toString();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(this.intVal);
		Text t = new Text(this.stringVal);
		t.write(arg0);
	}

}
