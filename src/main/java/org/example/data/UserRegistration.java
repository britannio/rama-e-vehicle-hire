package org.example.data;

import com.rpl.rama.RamaSerializable;

public class UserRegistration implements RamaSerializable {
  final String creationUUID;
  final String email;

  public UserRegistration(String creationUUID, String email) {
    this.creationUUID = creationUUID;
    this.email = email;
  }
}
