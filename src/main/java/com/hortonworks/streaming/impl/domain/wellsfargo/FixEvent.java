package com.hortonworks.streaming.impl.domain.wellsfargo;

import com.hortonworks.streaming.impl.domain.Event;

import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;

public class FixEvent extends Event {
    
    public enum FixEventEnum {
        fix1, fix2, fix3
    }
    
    private static Map<FixEventEnum, String> templates = new HashMap<FixEventEnum, String>(); 
    
    static { 
    	try {
    		InputStream is = SdrEvent.class.getResourceAsStream("fix_sample1.txt");
    		templates.put(FixEventEnum.fix1, IOUtils.toString( is , "UTF-8"));
    		is = SdrEvent.class.getResourceAsStream("fix_sample2.txt");
    		templates.put(FixEventEnum.fix2, IOUtils.toString( is , "UTF-8"));    		
    		is = SdrEvent.class.getResourceAsStream("fix_sample3.txt");
    		templates.put(FixEventEnum.fix3, IOUtils.toString( is , "UTF-8"));		
    	}
    	catch (Exception e) { 
            System.out.println("Exception: " + e.getMessage() );
            throw new RuntimeException("Unable to read FixEvent template file", e);
        }
    }
    
    private Random random = new Random();
    
    private static long tradeIdSeed = 0; 
    
    private static long dateStart=Timestamp.valueOf("2013-11-01 00:00:00").getTime();
    private static long dateOffset=Timestamp.valueOf("2013-12-31 00:00:00").getTime() - dateStart + 1;
        

    private FixEventEnum eventType; 
    private long tradeId;
    private Timestamp effectiveTimestamp;
    private int onetonine;
    
    
	public FixEvent( FixEventEnum eventType ) {
        this.eventType = eventType;
        tradeIdSeed ++;
        tradeId = tradeIdSeed;
        effectiveTimestamp = new Timestamp(dateStart + (long)(Math.random() * dateOffset));
        onetonine = random.nextInt(8) + 1;
	}

	
    @Override
	public String toString() {
        String template = templates.get(eventType);

        // String.valueOf(effectiveTimestamp)
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.sss");
        Calendar cal = Calendar.getInstance();
        cal.setTime(effectiveTimestamp);
        
        String effectiveTimestampFormatted = sdf.format(effectiveTimestamp);
        sdf = new SimpleDateFormat("yyyyMMdd");
        String effectiveDate = sdf.format(effectiveTimestamp);
        sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
        String effectiveTimestampNoMicro = sdf.format(effectiveTimestamp);
        
        template = template
            .replace("${onetonine}", String.valueOf(onetonine))
            .replace("${effectiveTimestamp}",effectiveTimestampFormatted)
            .replace("${effectiveDate}",effectiveDate)
            .replace("${effectiveTimestampNoMicro}",effectiveTimestampNoMicro);

        return template;
        // convert to XML
        //return new StvToXml(template).getXml();
    }
    
}
