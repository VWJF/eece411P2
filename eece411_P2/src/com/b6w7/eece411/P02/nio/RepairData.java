package com.b6w7.eece411.P02.nio;

import java.net.InetSocketAddress;
import java.util.Arrays;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;
import com.b6w7.eece411.P02.multithreaded.NodeCommands.Request;

public class RepairData { //  implements Comparable<RepairData> {

	private final ByteArrayWrapper key;
	private final InetSocketAddress destination;
	private final byte[] value;
	private final Request cmd;

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
        
        boolean inetComparison = false;
        if ( 0 == Handler.compareSocket(destination, otherRepair.destination ) )
        		inetComparison = true;
        
        boolean equals = key.equals( otherRepair.key) 
        		 		&& Arrays.equals(this.value, otherRepair.value)
        		 		&& (cmd == otherRepair.cmd)
        		 		&& inetComparison ;
        		 		//.destination)(destination.equals( ((RepairData) other).destination);
         return equals;
    }

    @Override
    public int hashCode()
    {
    	//TODO: Enum HashCode
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
	
//	@Override
//	public String toString(){
//
//		StringBuilder s = new StringBuilder();
//
//		//Show as Bytes
//		s.append("[key=>");
//		if (null != key) {
//			for (int i=0; i<key.; i++)
//				s.append(Integer.toString((key[i] & 0xff) + 0x100, 16).substring(1));
//		} else {
//			s.append("null");
//		}
//		s.append("]");
//		
//		//Show as String
////		s.append("[key-string=>");
////		if (null != data) {
////			s.append(new String(data));
////		} else {
////			s.append("null");
////		}
////		s.append("]");
//
//
//		return s.toString();
//	}

}
