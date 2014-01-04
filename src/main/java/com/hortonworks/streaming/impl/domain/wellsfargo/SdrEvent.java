package com.hortonworks.streaming.impl.domain.wellsfargo;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class SdrEvent extends WFBEvent {
    private static final String SDR1_FILENAME = "qaproject1_SDR_REPORTING_155098_TradeId_11000938_.txt";
    private static final String SDR2_FILENAME = "qaproject1_SDR_REPORTING_186661_TradeId_11008962_.txt";
    private static final String SDR3_FILENAME = "qaproject1_SDR_REPORTING_181391_TradeId_11007774_.txt";

    public enum SdrEventEnum {
        sdr_4k, sdr_16k, sdr_240k
    }
    
    private static Map<SdrEventEnum, WFBEvent.Template> templates = new HashMap<SdrEventEnum, WFBEvent.Template>(); 
    
    static { 
    	try {
    		InputStream is = SdrEvent.class.getResourceAsStream(SDR1_FILENAME);
    		templates.put( SdrEventEnum.sdr_4k, new WFBEvent.Template(IOUtils.toString( is , "UTF-8"), SDR1_FILENAME)) ;
    		is = SdrEvent.class.getResourceAsStream(SDR2_FILENAME);
    		templates.put( SdrEventEnum.sdr_16k, new WFBEvent.Template(IOUtils.toString( is , "UTF-8"), SDR2_FILENAME)) ;
    		is = SdrEvent.class.getResourceAsStream(SDR3_FILENAME);
    		templates.put( SdrEventEnum.sdr_240k, new WFBEvent.Template(IOUtils.toString( is , "UTF-8"), SDR3_FILENAME)) ;
    	}
    	catch (Exception e) {  
    		throw new RuntimeException( "Exception in SdrEvent." , e );
    	}
    }
    
    private static long effectiveDateStart=Timestamp.valueOf("2013-11-01 00:00:00").getTime();
    private static long effectiveDateOffset=Timestamp.valueOf("2013-12-31 00:00:00").getTime() - effectiveDateStart + 1;
    
    private static long terminationDateStart=Timestamp.valueOf("2013-11-01 00:00:00").getTime();
    private static long terminationDateOffset=Timestamp.valueOf("2013-12-31 00:00:00").getTime() - effectiveDateStart + 1;
    

    private Timestamp effectiveDate;
    private Timestamp terminationDate;
	
	public SdrEvent( SdrEventEnum eventType ) {
        effectiveDate = new Timestamp(effectiveDateStart + (long)(Math.random() * effectiveDateOffset));
        terminationDate = new Timestamp(terminationDateStart + (long)(Math.random() * terminationDateOffset));
        setTemplate(templates.get(eventType));
        setType("TXT");
	}

	
    @Override
	public String toString() {
    	String templateStr = getTemplate().getTemplatePayLoad();
        
        return templateStr
            .replace("${tradeId}", String.valueOf(this.getUuid()))
            .replace("${effectiveDate}",String.valueOf(effectiveDate))
            .replace("${terminationDate}",String.valueOf(terminationDate));
        
    }
    
}
