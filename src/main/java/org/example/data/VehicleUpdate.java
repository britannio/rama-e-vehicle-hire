package org.example.data;

import com.rpl.rama.RamaSerializable;

public class VehicleUpdate implements RamaSerializable {
  public final String vehicleId;
  public final Integer battery;
  public final LatLng location;

  public VehicleUpdate(String vehicleId, Integer battery, LatLng location) {
    this.vehicleId = vehicleId;
    this.battery = battery;
    this.location = location;
  }
}
