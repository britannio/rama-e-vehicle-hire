package org.example.data;

import com.rpl.rama.RamaSerializable;

public class VehicleCreate implements RamaSerializable {
  final String creationUUID;

  public VehicleCreate(String creationUUID) {
    this.creationUUID = creationUUID;
  }
}
