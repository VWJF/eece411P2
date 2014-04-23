package com.b6w7.eece411.P02.nio;

import java.net.InetSocketAddress;
import java.util.Arrays;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;
import com.b6w7.eece411.P02.multithreaded.NodeCommands.Request;

public class RepairData { //  implements Comparable<RepairData> {

	public final ByteArrayWrapper key;
	public final InetSocketAddress destination;
	public final byte[] value;
	public final Request cmd;

	public RepairData(ByteArrayWrapper node, byte[] data, InetSocketAddress dest, NodeCommands.Request request) {
		key = node;
		value = data;
		destination = dest;
		cmd = request;
	}
	

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ByteArrayWrapper))
        {
            return false;
        }
        RepairData otherRepair = (RepairData) other;
        
        boolean destinationComparison = false;
        if ( 0 == Handler.compareSocket(destination, otherRepair.destination ) )
        		destinationComparison = true;
        
        boolean equals = key.equals( otherRepair.key) 
        		 		&& Arrays.equals(this.value, otherRepair.value)
        		 		&& (cmd == otherRepair.cmd)
        		 		&& destinationComparison ;
         return equals;
    }

    @Override
    public int hashCode()
    {
    	//FIXME: Verify use of Enum.hashCode()
    	return key.hashCode() + value.hashCode() + destination.hashCode() + cmd.hashCode() ;
    }
    
//	@Override
//	public int compareTo(RepairData arg0) {
//		// TODO Auto-generated method stub
//		
//		int k =  key.compareTo(arg0.key);
//		int v =  value.compareTo(arg0.value);
//		int c =  cmd.compareTo(arg0.cmd);
//		int d =  Handler.compareSocket(destination, arg0.destination );
//		
//		return ;
//		
//		 
//	}
	
	@Override
	public String toString(){

		StringBuilder s = new StringBuilder();

		s.append("[key=>").append( key.toString() );
		s.append("] [value=>").append( new String(value) );
		s.append("] [destination=>").append( destination.toString() );
		s.append("] [request=>").append( cmd.toString() );
		//s.append(Integer.toString((cmd[i] & 0xff) + 0x100, 16).substring(1));
		s.append("]");
		
		//Show as Bytes
		s.append("[key=>");
		if (null != key) {
		} else {
			s.append("null");
		}
		s.append("]");

		return s.toString();
	}

}
