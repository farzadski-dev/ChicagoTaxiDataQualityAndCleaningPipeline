package dev.farzadski;

import static org.junit.jupiter.api.Assertions.*;

import dev.farzadski.bronze.enums.ColumnName;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ColumnNameEnumTest {
  @Test
  void provider_vendor_id_has_correct_raw_and_canonical_names() {
    assertEquals("VendorID", ColumnName.PROVIDER_VENDOR_ID.raw());
    assertEquals("PROVIDER_VENDOR_ID", ColumnName.PROVIDER_VENDOR_ID.canonical());
  }

  @Test
  void meter_engaged_at_has_correct_raw_and_canonical_names() {
    assertEquals("tpep_pickup_datetime", ColumnName.METER_ENGAGED_AT.raw());
    assertEquals("METER_ENGAGED_AT", ColumnName.METER_ENGAGED_AT.canonical());
  }

  @Test
  void meter_disengaged_at_has_correct_raw_and_canonical_names() {
    assertEquals("tpep_dropoff_datetime", ColumnName.METER_DISENGAGED_AT.raw());
    assertEquals("METER_DISENGAGED_AT", ColumnName.METER_DISENGAGED_AT.canonical());
  }

  @Test
  void fare_rate_code_has_correct_raw_and_canonical_names() {
    assertEquals("RatecodeID", ColumnName.FARE_RATE_CODE.raw());
    assertEquals("FARE_RATE_CODE", ColumnName.FARE_RATE_CODE.canonical());
  }

  @Test
  void payment_method_code_has_correct_raw_and_canonical_names() {
    assertEquals("payment_type", ColumnName.PAYMENT_METHOD_CODE.raw());
    assertEquals("PAYMENT_METHOD_CODE", ColumnName.PAYMENT_METHOD_CODE.canonical());
  }

  @Test
  void pickup_and_dropoff_location_columns_are_correct() {
    assertEquals("PULocationID", ColumnName.PICKUP_TAXI_ZONE_ID.raw());
    assertEquals("PICKUP_TAXI_ZONE_ID", ColumnName.PICKUP_TAXI_ZONE_ID.canonical());

    assertEquals("DOLocationID", ColumnName.DROP_OFF_TAXI_ZONE_ID.raw());
    assertEquals("DROP_OFF_TAXI_ZONE_ID", ColumnName.DROP_OFF_TAXI_ZONE_ID.canonical());
  }

  @Test
  void money_columns_have_usd_suffix() {
    assertTrue(ColumnName.BASE_FARE_AMOUNT_USD.canonical().endsWith("_USD"));
    assertTrue(ColumnName.MTA_TAX_AMOUNT_USD.canonical().endsWith("_USD"));
    assertTrue(ColumnName.TOTAL_CHARGE_AMOUNT_USD.canonical().endsWith("_USD"));
  }

  @Test
  void raw_column_names_must_be_unique() {
    Set<String> rawNames = new HashSet<>();

    Arrays.stream(ColumnName.values())
        .map(ColumnName::raw)
        .forEach(raw -> assertTrue(rawNames.add(raw), "DUPLICATE_RAW_COLUMN_NAME_FOUND: " + raw));
  }

  @Test
  void canonical_column_names_must_be_unique() {
    Set<String> canonicalNames = new HashSet<>();

    Arrays.stream(ColumnName.values())
        .map(ColumnName::canonical)
        .forEach(
            canonical ->
                assertTrue(
                    canonicalNames.add(canonical),
                    "DUPLICATE_CANONICAL_COLUMN_NAME_FOUND: " + canonical));
  }
}
