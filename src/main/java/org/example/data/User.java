package org.example.data;

import com.rpl.rama.RamaSerializable;

public class User implements RamaSerializable {
  public final String userId;
  public final String email;
  public final String creationUUID;

  public User(String userId, String email, String creationUUID) {
    this.userId = userId;
    this.email = email;
    this.creationUUID = creationUUID;
  }
}

