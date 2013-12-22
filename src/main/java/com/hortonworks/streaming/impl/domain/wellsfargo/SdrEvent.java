package com.hortonworks.streaming.impl.domain.wellsfargo;

import com.hortonworks.streaming.impl.domain.Event;
import com.hortonworks.streaming.impl.domain.wellsfargo.FixEvent.FixEventEnum;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class SdrEvent extends Event {
    
    public enum SdrEventEnum {
        sdr_4k, sdr_16k, sdr_240k
    }
    
    private static Map<SdrEventEnum, String> templates = new HashMap<SdrEventEnum, String>(); 
    
    static { 
    	try {
    		InputStream is = SdrEvent.class.getResourceAsStream("qaproject1_SDR_REPORTING_155098_TradeId_11000938_.txt");
    		templates.put( SdrEventEnum.sdr_4k, IOUtils.toString( is , "UTF-8")) ;
    		is = SdrEvent.class.getResourceAsStream("qaproject1_SDR_REPORTING_186661_TradeId_11008962_.txt");
    		templates.put( SdrEventEnum.sdr_16k, IOUtils.toString( is , "UTF-8")) ;
    		is = SdrEvent.class.getResourceAsStream("qaproject1_SDR_REPORTING_181391_TradeId_11007774_.txt");
    		templates.put( SdrEventEnum.sdr_240k, IOUtils.toString( is , "UTF-8")) ;
    	}
    	catch (Exception e) {  
    		throw new RuntimeException( "Exception in SdrEvent." , e );
    	}
    }
    
    private static long tradeIdSeed = 0; 
    
    private static long effectiveDateStart=Timestamp.valueOf("2013-11-01 00:00:00").getTime();
    private static long effectiveDateOffset=Timestamp.valueOf("2013-12-31 00:00:00").getTime() - effectiveDateStart + 1;
    
    private static long terminationDateStart=Timestamp.valueOf("2013-11-01 00:00:00").getTime();
    private static long terminationDateOffset=Timestamp.valueOf("2013-12-31 00:00:00").getTime() - effectiveDateStart + 1;
    

    private SdrEventEnum eventType; // should be an enum
    private long tradeId;
    private Timestamp effectiveDate;
    private Timestamp terminationDate;
    
	public SdrEvent( SdrEventEnum eventType ) {
        this.eventType = eventType;
        tradeIdSeed ++;
        tradeId = tradeIdSeed;
        effectiveDate = new Timestamp(effectiveDateStart + (long)(Math.random() * effectiveDateOffset));
        terminationDate = new Timestamp(terminationDateStart + (long)(Math.random() * terminationDateOffset));
	}

	
    @Override
	public String toString() {
        String template = templates.get( eventType );
        
        return template
            .replace("${tradeId}", String.valueOf(tradeId))
            .replace("${effectiveDate}",String.valueOf(effectiveDate))
            .replace("${terminationDate}",String.valueOf(terminationDate));
        
    }
    
}
