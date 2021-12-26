/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nyc_borough_compare2;

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
	private String borough;
    private String streetName;
    


private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");

    public int compareTo(KeyCompare k) {
			
			int result = 0 ;
			try {
			 result = this.borough.compareTo(k.getBorough());
			if(0 == result) {
	         result = -1*this.streetName.compareTo(k.getStreetName());

	}
			}
			catch (Exception e) {
			      
			    }
			return result;
		}
		
    
    
    @Override
	public String toString() {
		return (new StringBuilder().append(borough).append("\t")
				.append(streetName)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		borough = WritableUtils.readString(dataInput);
		streetName = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, borough);
		WritableUtils.writeString(dataOutput, streetName);
	}



	public String getStreetName() {
		return streetName;
	}


	public void setStreetName(String streetName) {
		this.streetName = streetName;
	}


	public String getBorough() {
		return borough;
	}


	public void setBorough(String borough) {
		this.borough = borough;
	}




	





}
