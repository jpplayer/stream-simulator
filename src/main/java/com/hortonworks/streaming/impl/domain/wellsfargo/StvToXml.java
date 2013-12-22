package com.hortonworks.streaming.impl.domain.wellsfargo;

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a very quick and dirty hack to write something that
 * is able to convert STV to XML
 */

public class StvToXml {

    private final String stv;

    public StvToXml(String stv) {
        this.stv = stv;
    }

    public String getXml() {

        // This is probably not a keeper :P

        XMLOutputFactory factory = XMLOutputFactory.newInstance();

        StringWriter sw = new StringWriter();

        try {
            XMLStreamWriter writer = new IndentingXMLStreamWriter(factory.createXMLStreamWriter(
                    sw));
            writer.writeStartDocument();
            writer.writeStartElement("message");
            writer.writeStartElement("data");

            // While we build up the document we will build up the arrays and objects to
            // build at the end
            Map<String, List<String>> arrays = new HashMap<String, List<String>>();
            Map<String, Map<String, String>> objects = new HashMap<String, Map<String, String>>();

            //System.lineSeparator())
            for (String line : stv.split( new String("\n") ) ) {
                String key = line.substring(line.indexOf('{') + 1, line.indexOf('}'));
                String value = null;
                if (line.split("=").length > 1)
                    value = line.split("=")[1];

                // Keys can mean a few things
                if (key.contains("[")) {
                    // We have an array for the time being we are going to
                    // assume they coming in order
                    String arrayName = key.substring(0, key.indexOf('['));
                    if (!arrays.containsKey(arrayName)) {
                        arrays.put(arrayName, new ArrayList());
                    }
                    arrays.get(arrayName).add(value);
                } else if (key.contains(".")) {
                    // We have an array for the time being we are going to
                    // assume they coming in order
                    String objectName = key.substring(0, key.indexOf('.'));
                    if (!objects.containsKey(objectName)) {
                        objects.put(objectName, new HashMap<String, String>());
                    }
                    objects.get(objectName).put(key.substring(key.indexOf('.') + 1), value);
                } else {
                    // we have a basic property which we can treat as an attribute
                    if (value != null) {
                        writer.writeStartElement("property");
                        writer.writeAttribute("name", key);
                        writer.writeAttribute("value", value.trim());
                        writer.writeEndElement();
                    }
                }
            }

            // We have processed all the STV now lets dump the objects and arrays
            for (String key : arrays.keySet()) {
                writer.writeStartElement(key);
                for (String value : arrays.get(key)) {
                    writer.writeStartElement("entry");
                    writer.writeAttribute("value", value.trim());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }

            // We have processed all the STV now lets dump the objects and arrays
            for (String key : objects.keySet()) {
                writer.writeStartElement(key);
                for (String hashKey : objects.get(key).keySet()) {
                    writer.writeStartElement("property");
                    writer.writeAttribute("name", hashKey);
                    writer.writeAttribute("value", objects.get(key).get(hashKey).trim());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }


            writer.writeEndElement();
            writer.writeEndDocument();

            writer.flush();
            writer.close();

            return sw.toString();

        } catch (Exception e) {
            throw new RuntimeException("Unable build XML writer", e);
        }


    }
}
