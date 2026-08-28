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
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Simple DOM based config file writer.
 * <p>
 * This class can init/load existing ozone-default-generated.xml fragments
 * and append new entries and write to the file system.
 */
public class ConfigFileAppender {

  private XMLConfiguration config;

  /**
   * Initialize a new ozone-site.xml structure with empty content.
   */
  public void init() {
    config = new XMLConfiguration();
  }

  /**
   * Load existing ozone-site.xml content and parse the DOM tree.
   */
  public void load(InputStream stream) {
    try {
      config = XMLConfiguration.readFromXml(stream);
    } catch (Exception ex) {
      throw new ConfigurationException("Can't load existing configuration", ex);
    }
  }

  /**
   * Add configuration fragment.
   */
  public void addConfig(String key, String defaultValue, String description,
      ConfigTag[] tags) {
    String tagsAsString = Arrays.stream(tags).map(Enum::name)
        .collect(Collectors.joining(", "));

    Property prop = new Property();
    prop.setName(key);
    prop.setValue(defaultValue);
    prop.setDescription(description);
    prop.setTag(tagsAsString);
    config.addProperty(prop);
  }

  /**
   * Write out the XML content to a writer.
   */
  public void write(Writer writer) {
    try {
      config.writeToXml(writer);
    } catch (Exception e) {
      throw new ConfigurationException("Can't write the configuration xml", e);
    }
  }
}
