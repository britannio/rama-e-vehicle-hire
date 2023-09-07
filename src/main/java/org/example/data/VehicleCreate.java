package org.example.data;

import com.rpl.rama.RamaSerializable;

public class VehicleCreate implements RamaSerializable {
  public final String creationUUID;
  public final String vehicleId;

  public VehicleCreate(String creationUUID, String vehicleId) {
    this.creationUUID = creationUUID;
    this.vehicleId = vehicleId;
  }

}
