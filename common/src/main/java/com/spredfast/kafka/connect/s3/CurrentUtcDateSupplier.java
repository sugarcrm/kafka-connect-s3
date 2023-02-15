package com.spredfast.kafka.connect.s3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * We store block objects with a date prefix just to make finding them and navigating around the
 * bucket a bit easier. The meaning of the date is "when this block was uploaded".
 */
public class CurrentUtcDateSupplier implements Supplier<String> {
  private final DateFormat dateFormat;

  public CurrentUtcDateSupplier() {
    final TimeZone UTC = TimeZone.getTimeZone("UTC");
    dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    dateFormat.setTimeZone(UTC);
  }

  @Override
  public String get() {
    return dateFormat.format(new Date());
  }
}
