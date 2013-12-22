package com.hortonworks.streaming.impl.domain.wellsfargo;

import com.hortonworks.streaming.impl.domain.Event;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.commons.io.FileUtils;

public class SdrEvent extends Event {
    
    private static String msg1;
    static { 
    	try {
    		InputStream is = SdrEvent.class.getResourceAsStream("qaproject1_SDR_REPORTING_155098_TradeId_11000938_.txt");
    		msg1 = org.apache.commons.io.IOUtils.toString( is , "UTF-8");
    	}
    	catch (Exception e) { System.out.println("Exception: " + e.getMessage() );}
    }
    
    private static long tradeIdSeed = 0; 
    
    private static long effectiveDateStart=Timestamp.valueOf("2012-01-01 00:00:00").getTime();
    private static long effectiveDateOffset=Timestamp.valueOf("2013-01-01 00:00:00").getTime() - effectiveDateStart + 1;
    
    private static long terminationDateStart=Timestamp.valueOf("2012-01-01 00:00:00").getTime();
    private static long terminationDateOffset=Timestamp.valueOf("2013-01-01 00:00:00").getTime() - effectiveDateStart + 1;
    
    
    private long tradeId;
    private Timestamp effectiveDate;
    private Timestamp terminationDate;
    
	public SdrEvent() {
        tradeIdSeed ++;
        tradeId = tradeIdSeed;
        effectiveDate = new Timestamp(effectiveDateStart + (long)(Math.random() * effectiveDateOffset));
        terminationDate = new Timestamp(terminationDateStart + (long)(Math.random() * terminationDateOffset));
	}

	
    @Override
	public String toString() {
        
        return msg1
            .replace("${tradeId}", String.valueOf(tradeId))
            .replace("${effectiveDate}",String.valueOf(effectiveDate))
            .replace("${terminationDate}",String.valueOf(terminationDate));
        
    }
    
}
