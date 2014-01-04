package com.hortonworks.streaming.impl.domain.wellsfargo;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class ScritturaEvent extends WFBEvent {
    private static final String _11009015_FILENAME = "scrittura_confirmACK_11009015.xml";
    
    private static Map<CommoditiesEnum, WFBEvent.Template> templates = new HashMap<CommoditiesEnum, Template>(); 

    public enum CommoditiesEnum {
        _11009015
    }
    
    static { 
    	try {
    		InputStream is = SdrEvent.class.getResourceAsStream(_11009015_FILENAME);
    		templates.put(CommoditiesEnum._11009015, new WFBEvent.Template(IOUtils.toString( is , "UTF-8"), _11009015_FILENAME)) ;
    	}
    	catch (Exception e) { System.out.println("Exception: " + e.getMessage() );}
    }
        
    private static long effectiveDateStart=Timestamp.valueOf("2012-01-01 00:00:00").getTime();
    private static long effectiveDateOffset=Timestamp.valueOf("2013-01-01 00:00:00").getTime() - effectiveDateStart + 1;
    
    private static long terminationDateStart=Timestamp.valueOf("2012-01-01 00:00:00").getTime();
    private static long terminationDateOffset=Timestamp.valueOf("2013-01-01 00:00:00").getTime() - effectiveDateStart + 1;
    
    
    private Timestamp effectiveDate;
    private Timestamp terminationDate;

	public ScritturaEvent() {
        effectiveDate = new Timestamp(effectiveDateStart + (long)(Math.random() * effectiveDateOffset));
        terminationDate = new Timestamp(terminationDateStart + (long)(Math.random() * terminationDateOffset));
        setTemplate(templates.get(CommoditiesEnum._11009015));
        setType("XML");
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
