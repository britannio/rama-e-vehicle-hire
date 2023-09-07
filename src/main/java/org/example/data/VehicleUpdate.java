package org.example.data;

public class VehicleUpdate {
  final Integer battery;
  final LatLng location;;

  public VehicleUpdate(Integer battery, LatLng location) {
    this.battery = battery;
    this.location = location;
  }
}
