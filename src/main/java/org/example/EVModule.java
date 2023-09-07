package org.example;

import com.rpl.rama.Depot;
import com.rpl.rama.PState;
import com.rpl.rama.RamaModule;
import com.rpl.rama.SubindexOptions;
import com.rpl.rama.helpers.TopologyUtils;
import com.rpl.rama.module.StreamTopology;
import org.example.data.LatLng;

public class EVModule implements RamaModule {

  public static class ExtractVehicleId extends TopologyUtils.ExtractJavaField {
    public ExtractVehicleId() {
      super("vehicleId");
    }
  }

  public static class ExtractUserId extends TopologyUtils.ExtractJavaField {
    public ExtractUserId() {
      super("userId");
    }
  }

  public static class ExtractUserEmail extends TopologyUtils.ExtractJavaField {
    public ExtractUserEmail() {
      super("email");
    }
  }

  private static void declareUsersTopology(Topologies topologies) {
    StreamTopology users = topologies.stream("users");
    users.pstate("$$users",
        PState.mapSchema(
            String.class, // userId
            PState.fixedKeysSchema(
                "email", String.class,
                "creationUUID", String.class
            )
        )
    );
    users.pstate("$$userRide", PState.mapSchema(
        String.class, // userId
        PState.fixedKeysSchema(
            "vehicleId", String.class,
            "rideId", String.class
        )
    ));
    users.pstate("$$userRideHistory", PState.mapSchema(
        String.class, // userId
        PState.listSchema(PState.fixedKeysSchema(
            "rideId", String.class,
            "vehicleId", String.class,
            "startLocation", LatLng.class,
            "endLocation", LatLng.class,
            "startTimestamp", Long.class,
            "endTimestamp", Long.class,
            "route", PState.listSchema(LatLng.class)
        ))
    ));

  }

  private static void declareVehiclesTopology(Topologies topologies) {
    StreamTopology vehicles = topologies.stream("vehicles");
    vehicles.pstate("$$vehicles", PState.mapSchema(
        String.class, // vehicleId
        PState.fixedKeysSchema(
            "battery", Integer.class,
            "location", String.class,
            "creationUUID", String.class
        )
    ));
    vehicles.pstate("$$vehicleLocationHistory",
        PState.mapSchema(
            String.class,
            PState.listSchema(
                PState.fixedKeysSchema(
                    "location", LatLng.class,
                    "timestamp", Long.class
                )
            )
        ));
    vehicles.pstate("$$vehicleRide",
        PState.mapSchema(
            String.class, // vehicleId
            PState.fixedKeysSchema(
                "rideId", String.class,
                "riderId", String.class,
                "startLocation", LatLng.class,
                "endLocation", LatLng.class,
                "startTimestamp", Long.class
            )
        )
    );
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*vehicleCreate", Depot.hashBy(ExtractVehicleId.class));
    setup.declareDepot("*vehicleUpdate", Depot.hashBy(ExtractVehicleId.class));
    setup.declareDepot("*userRegistration", Depot.hashBy(ExtractUserEmail.class));
    setup.declareDepot("*rideBegin", Depot.hashBy(ExtractVehicleId.class));
    setup.declareDepot("*rideEnd", Depot.hashBy(ExtractVehicleId.class));

    declareUsersTopology(topologies);
    declareVehiclesTopology(topologies);

  }
}
