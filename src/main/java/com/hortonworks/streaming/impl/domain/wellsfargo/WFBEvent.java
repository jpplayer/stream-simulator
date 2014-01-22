package com.hortonworks.streaming.impl.domain.wellsfargo;

import java.util.UUID;

import com.hortonworks.streaming.impl.domain.Event;

public class WFBEvent extends Event {
	private UUID uuid = UUID.randomUUID();
	private Template template;
	private String type  = "";
	private String effectiveDate = "";
	private String terminationDate = "";
	
	public Template getTemplate() {
		return template;
	}
	public void setTemplate(Template template) {
		this.template = template;
	}
	
	public UUID getUuid() {
		return uuid;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}

	public void setEffectiveDate(String effectiveDate) {
		this.effectiveDate = effectiveDate;
	}

	public String getEffectiveDate() {
		return effectiveDate;
	}


	public void setTerminationDate(String terminationDate) {
		this.terminationDate = terminationDate;
	}
	public String getTerminationDate() {
		return terminationDate;
	}

	/* Carries file name and actual event template */
	public static class Template {
		private String templatePayLoad = "";
		private String fileName = "";

		Template(String templatePayLoad, String fileName) {
			this.templatePayLoad = templatePayLoad;
			this.fileName = fileName;
		}

		public String getFileName() {
			return fileName;
		}

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}

		public String getTemplatePayLoad() {
			return templatePayLoad;
		}

		public void setTemplatePayLoad(String templatePayLoad) {
			this.templatePayLoad = templatePayLoad;
		}
		
    }	
	
}
