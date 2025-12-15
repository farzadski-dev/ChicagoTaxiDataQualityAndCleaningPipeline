package dev.farzadski.bronze.enums;

public enum ColumnName {
  PROVIDER_VENDOR_ID("VendorID", "PROVIDER_VENDOR_ID"),

  METER_ENGAGED_AT("tpep_pickup_datetime", "METER_ENGAGED_AT"),
  METER_DISENGAGED_AT("tpep_dropoff_datetime", "METER_DISENGAGED_AT"),

  PASSENGER_COUNT("passenger_count", "PASSENGER_COUNT"),
  TRIP_DISTANCE_MILES("trip_distance", "TRIP_DISTANCE_MILES"),

  FARE_RATE_CODE("RatecodeID", "FARE_RATE_CODE"),
  PAYMENT_METHOD_CODE("payment_type", "PAYMENT_METHOD_CODE"),

  STORED_BEFORE_TRANSMISSION_FLAG("store_and_fwd_flag", "STORED_BEFORE_TRANSMISSION_FLAG"),

  PICKUP_TAXI_ZONE_ID("PULocationID", "PICKUP_TAXI_ZONE_ID"),
  DROP_OFF_TAXI_ZONE_ID("DOLocationID", "DROP_OFF_TAXI_ZONE_ID"),

  BASE_FARE_AMOUNT_USD("fare_amount", "BASE_FARE_AMOUNT_USD"),
  EXTRA_CHARGES_AMOUNT_USD("extra", "EXTRA_CHARGES_AMOUNT_USD"),
  MTA_TAX_AMOUNT_USD("mta_tax", "MTA_TAX_AMOUNT_USD"),
  TIP_AMOUNT_USD("tip_amount", "TIP_AMOUNT_USD"),
  TOLLS_AMOUNT_USD("tolls_amount", "TOLLS_AMOUNT_USD"),
  IMPROVEMENT_SURCHARGE_AMOUNT_USD("improvement_surcharge", "IMPROVEMENT_SURCHARGE_AMOUNT_USD"),
  TOTAL_CHARGE_AMOUNT_USD("total_amount", "TOTAL_CHARGE_AMOUNT_USD"),
  STATE_CONGESTION_SURCHARGE_AMOUNT_USD(
      "congestion_surcharge", "STATE_CONGESTION_SURCHARGE_AMOUNT_USD"),
  AIRPORT_PICKUP_FEE_AMOUNT_USD("Airport_fee", "AIRPORT_PICKUP_FEE_AMOUNT_USD"),
  MTA_CBD_CONGESTION_FEE_AMOUNT_USD("cbd_congestion_fee", "MTA_CBD_CONGESTION_FEE_AMOUNT_USD");

  private final String raw;
  private final String canonical;

  ColumnName(String raw, String canonical) {
    this.raw = raw;
    this.canonical = canonical;
  }

  public String raw() {
    return raw;
  }

  public String canonical() {
    return canonical;
  }
}
