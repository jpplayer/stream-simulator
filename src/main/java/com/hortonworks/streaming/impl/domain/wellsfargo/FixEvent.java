package com.hortonworks.streaming.impl.domain.wellsfargo;

import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;

public class FixEvent extends WFBEvent {
    private static final String FIX1_FILENAME = "fix_sample1.txt";
    private static final String FIX2_FILENAME = "fix_sample2.txt";
    private static final String FIX3_FILENAME = "fix_sample3.txt";
	
    public enum FixEventEnum {
        fix1, fix2, fix3
    }
    
    private static Map<FixEventEnum, WFBEvent.Template> templates = new HashMap<FixEventEnum, Template>(); 
    
    static { 
    	try {
    		InputStream is = SdrEvent.class.getResourceAsStream(FIX1_FILENAME);
    		templates.put(FixEventEnum.fix1, new WFBEvent.Template(IOUtils.toString( is , "UTF-8"), FIX1_FILENAME));
    		is = SdrEvent.class.getResourceAsStream(FIX2_FILENAME);
    		templates.put(FixEventEnum.fix2, new WFBEvent.Template(IOUtils.toString( is , "UTF-8"), FIX2_FILENAME));    		
    		is = SdrEvent.class.getResourceAsStream(FIX3_FILENAME);
    		templates.put(FixEventEnum.fix3, new WFBEvent.Template(IOUtils.toString( is , "UTF-8"), FIX3_FILENAME));		
    	}
    	catch (Exception e) { 
            System.out.println("Exception: " + e.getMessage() );
            throw new RuntimeException("Unable to read FixEvent template file", e);
        }
    }
    
    private Random random = new Random();
    
    private static long dateStart=Timestamp.valueOf("2013-11-01 00:00:00").getTime();
    private static long dateOffset=Timestamp.valueOf("2013-12-31 00:00:00").getTime() - dateStart + 1;
        
    private Timestamp effectiveTimestamp;
    private int onetonine;
    
	public FixEvent( FixEventEnum eventType ) {
        effectiveTimestamp = new Timestamp(dateStart + (long)(Math.random() * dateOffset));
        onetonine = random.nextInt(8) + 1;
        setTemplate(templates.get(eventType));
        setType("FIX");
	}

	
    @Override
	public String toString() {
    	String templateStr = getTemplate().getTemplatePayLoad();

        // String.valueOf(effectiveTimestamp)
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.sss");
        Calendar cal = Calendar.getInstance();
        cal.setTime(effectiveTimestamp);
        
        String effectiveTimestampFormatted = sdf.format(effectiveTimestamp);
        sdf = new SimpleDateFormat("yyyyMMdd");
        String effectiveDate = sdf.format(effectiveTimestamp);
        sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
        String effectiveTimestampNoMicro = sdf.format(effectiveTimestamp);
        
        templateStr = templateStr
            .replace("${onetonine}", String.valueOf(onetonine))
            .replace("${effectiveTimestamp}",effectiveTimestampFormatted)
            .replace("${effectiveDate}",effectiveDate)
            .replace("${effectiveTimestampNoMicro}",effectiveTimestampNoMicro);

        return templateStr;
        // convert to XML
        //return new StvToXml(template).getXml();
    }
    
}
