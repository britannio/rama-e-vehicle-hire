package org.example.data;

import com.rpl.rama.RamaSerializable;

public class Vehicle implements RamaSerializable {
  public final String id;
  public final LatLng location;

  public Vehicle(String id, LatLng location) {
    this.id = id;
    this.location = location;
  }
}
