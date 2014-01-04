package com.hortonworks.streaming.impl.domain.wellsfargo;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class CommoditiesEvent extends WFBEvent {
    private static final String _3587038_FILENAME = "commodities_3587038.xml";
    
    private static Map<CommoditiesEnum, WFBEvent.Template> templates = new HashMap<CommoditiesEnum, Template>(); 

    public enum CommoditiesEnum {
        _3587038
    }

    static { 
    	try {
    		InputStream is = SdrEvent.class.getResourceAsStream(_3587038_FILENAME);
    		templates.put(CommoditiesEnum._3587038, new WFBEvent.Template(IOUtils.toString( is , "UTF-8"), _3587038_FILENAME)) ;
    	}
    	catch (Exception e) { System.out.println("Exception: " + e.getMessage() );}
    }
    
    private static long effectiveDateStart=Timestamp.valueOf("2012-01-01 00:00:00").getTime();
    private static long effectiveDateOffset=Timestamp.valueOf("2013-01-01 00:00:00").getTime() - effectiveDateStart + 1;
    
    private static long terminationDateStart=Timestamp.valueOf("2012-01-01 00:00:00").getTime();
    private static long terminationDateOffset=Timestamp.valueOf("2013-01-01 00:00:00").getTime() - effectiveDateStart + 1;
    
    
    private Timestamp effectiveDate;
    private Timestamp terminationDate;

	public CommoditiesEvent() {
        effectiveDate = new Timestamp(effectiveDateStart + (long)(Math.random() * effectiveDateOffset));
        terminationDate = new Timestamp(terminationDateStart + (long)(Math.random() * terminationDateOffset));
        setTemplate(templates.get(CommoditiesEnum._3587038));
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
