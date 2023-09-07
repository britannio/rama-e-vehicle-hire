package org.example.data;

import com.rpl.rama.RamaSerializable;

public class RideEnd implements RamaSerializable {
  public final String userId;
  public final String vehicleId;

  public RideEnd(String userId, String vehicleId) {
    this.userId = userId;
    this.vehicleId = vehicleId;
  }
}
