package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;

public @Data class VehicleCreate implements RamaSerializable {
  public final String creationUUID;
  public final String vehicleId;
}
