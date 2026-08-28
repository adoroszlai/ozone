/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.conf;

import java.io.InputStream;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.util.JAXBSource;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import org.apache.hadoop.util.XMLUtils;

/**
 * Class to marshall/un-marshall configuration from xml files.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "configuration")
public class XMLConfiguration {

  @XmlElement(name = "property", type = Property.class)
  private List<Property> properties = new ArrayList<>();

  public XMLConfiguration() {
  }

  public XMLConfiguration(List<Property> properties) {
    this.properties = new ArrayList<>(properties);
  }

  public static XMLConfiguration readFromXml(InputStream input) throws JAXBException {
    JAXBContext context = JAXBContext.newInstance(XMLConfiguration.class);
    Unmarshaller um = context.createUnmarshaller();

    return (XMLConfiguration) um.unmarshal(input);
  }

  public static List<Property> readPropertyFromXml(URL url) throws JAXBException {
    JAXBContext context = JAXBContext.newInstance(XMLConfiguration.class);
    Unmarshaller um = context.createUnmarshaller();

    XMLConfiguration config = (XMLConfiguration) um.unmarshal(url);
    return config.getProperties();
  }

  public void writeToXml(Writer writer) throws JAXBException, TransformerException {
    Collections.sort(properties);

    JAXBContext context = JAXBContext.newInstance(XMLConfiguration.class);
    JAXBSource source = new JAXBSource(context, this);
    TransformerFactory factory = XMLUtils.newSecureTransformerFactory();
    Transformer transformer = factory.newTransformer();
    transformer.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.name());
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    StreamResult result = new StreamResult(writer);
    transformer.transform(source, result);
  }

  public void addProperty(Property property) {
    properties.add(property);
  }

  public List<Property> getProperties() {
    return Collections.unmodifiableList(properties);
  }
}
