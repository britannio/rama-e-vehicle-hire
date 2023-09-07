package org.example.data;

import com.rpl.rama.RamaSerializable;

public class Vehicle implements RamaSerializable {
  public final String vehicleId;
  public final LatLng location;
  public final String creationUUID;

  public Vehicle(String vehicleId, LatLng location, String creationUUID) {
    this.vehicleId = vehicleId;
    this.location = location;
    this.creationUUID = creationUUID;
  }
}
