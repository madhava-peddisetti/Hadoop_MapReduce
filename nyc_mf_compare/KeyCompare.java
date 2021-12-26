/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nyc_mf_compare;

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
	private String mf;
    private String conFactor;
    


private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");

    public int compareTo(KeyCompare k) {
			
			int result = 0 ;
			try {
			 result = this.mf.compareTo(k.getMf());
			if(0 == result) {
	         result = -1*this.conFactor.compareTo(k.getConFactor());

	}
			}
			catch (Exception e) {
			      
			    }
			return result;
		}
		
    
    
    @Override
	public String toString() {
		return (new StringBuilder().append(mf).append("\t")
				.append(conFactor)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		mf = WritableUtils.readString(dataInput);
		conFactor = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, mf);
		WritableUtils.writeString(dataOutput, conFactor);
	}



	public String getConFactor() {
		return conFactor;
	}


	public void setConFactor(String conFactor) {
		this.conFactor = conFactor;
	}


	public String getMf() {
		return mf;
	}


	public void setMf(String mf) {
		this.mf = mf;
	}




	





}
