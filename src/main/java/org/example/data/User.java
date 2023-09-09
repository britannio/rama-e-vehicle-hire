package org.example.data;

import com.rpl.rama.RamaSerializable;
import lombok.Data;

public @Data class User implements RamaSerializable {
  public final String userId;
  public final String email;
  public final String creationUUID;
}

