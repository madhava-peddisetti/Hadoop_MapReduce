/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nyc_crash_compare;

import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class KeyCompare implements WritableComparable<KeyCompare>{
    
    
   // private Date crashDate = new Date();
	private String crashDate;
    private String streetName;
    


private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");

    public int compareTo(KeyCompare k) {
			
			int result = 0 ;
			try {
			 result = this.streetName.compareTo(k.getStreetName());
			if(0 == result) {
	         result = -1*this.crashDate.compareTo(k.getCrashDate());

	}
			}
			catch (Exception e) {
			      
			    }
			return result;
		}
		
    
    
    @Override
	public String toString() {
		return (new StringBuilder().append(streetName).append("\t")
				.append(crashDate)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		streetName = WritableUtils.readString(dataInput);
		crashDate = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, streetName);
		WritableUtils.writeString(dataOutput, crashDate);
	}



	public String getStreetName() {
		return streetName;
	}


	public void setStreetName(String streetName) {
		this.streetName = streetName;
	}


	public String getCrashDate() {
		return crashDate;
	}


	public void setCrashDate(String crashDate) {
		this.crashDate = crashDate;
	}




	





}
