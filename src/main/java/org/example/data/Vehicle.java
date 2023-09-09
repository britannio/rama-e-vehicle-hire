package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;


public @Data class Vehicle implements RamaSerializable {
  public final String vehicleId;
  public final Integer battery;
  public final LatLng location;
  public final String creationUUID;
}
