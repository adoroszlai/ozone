/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.endpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

import java.util.Objects;

/**
 * SAX filter to force namespace usage on the root element.
 * <p>
 * This filter will read the XML content as namespace qualified content
 * independent of the current namespace usage.
 */
public class XmlNamespaceFilter extends XMLFilterImpl {

  private static final Logger LOG = LoggerFactory.getLogger(XmlNamespaceFilter.class);

  private final String namespace;
  private final String rootElement;

  /**
   * Create the filter.
   *
   * @param namespace to add to the root element
   */
  public XmlNamespaceFilter(String namespace, String root) {
    this.namespace = namespace;
    this.rootElement = root;
  }

  @Override
  public void startElement(String uri, String localName, String qName,
      Attributes atts) throws SAXException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("startElement uri:{} local:{} qName:{}", uri, localName, qName);
    }
    super.startElement(getNamespace(uri, qName), localName, qName, atts);
  }

  @Override
  public void endElement(String uri, String localName, String qName)
      throws SAXException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("endElement uri:{} local:{} qName:{}", uri, localName, qName);
    }
    super.endElement(getNamespace(uri, qName), localName, qName);
  }

  private String getNamespace(String uri, String qName) {
    return Objects.equals(rootElement, qName) ? namespace : uri;
  }
}
