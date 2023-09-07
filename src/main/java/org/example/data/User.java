package org.example.data;

import com.rpl.rama.RamaSerializable;

public record User(String userId, String email, String creationUUID) implements RamaSerializable {
}

