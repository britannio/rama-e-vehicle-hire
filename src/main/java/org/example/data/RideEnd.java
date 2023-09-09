package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;

public @Data class RideEnd implements RamaSerializable {
  public final String userId;
  public final String vehicleId;
}
