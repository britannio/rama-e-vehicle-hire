package org.example.data;

import com.rpl.rama.RamaSerializable;

public record LatLng(Long latitude, Long longitude) implements RamaSerializable {
}
