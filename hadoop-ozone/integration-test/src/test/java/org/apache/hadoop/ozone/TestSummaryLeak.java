package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.HddsUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** Testing HDDS-11551. */
class TestSummaryLeak {

  @Test
  void leaking() {
    final Class<?> cls = getClass();
    HddsUtils.reportLeak(cls, null, LoggerFactory.getLogger(cls));
  }
}
