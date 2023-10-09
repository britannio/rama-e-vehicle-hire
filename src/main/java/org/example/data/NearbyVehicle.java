package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;

public @Data class NearbyVehicle implements RamaSerializable {
  public final String vehicleId;
  // Distance from the reference location to the vehicle in meters.
  public final double distance;

}
