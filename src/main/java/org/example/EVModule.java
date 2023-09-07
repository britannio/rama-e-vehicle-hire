package org.example;

import com.rpl.rama.*;
import com.rpl.rama.helpers.TopologyUtils;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import org.example.data.LatLng;

import java.util.UUID;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

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
            "userId", String.class,
            "rideId", String.class,
            "vehicleId", String.class,
            "startLocation", LatLng.class,
            "endLocation", LatLng.class,
            "startTimestamp", Long.class,
            "endTimestamp", Long.class,
            "route", PState.listSchema(LatLng.class)
        ))
    ));
    users.pstate("$$emailToUserId", PState.mapSchema(
        String.class, // email
        String.class // userId
    ));

    users.source("*userRegistration").out("*out")
        .macro(extractJavaFields("*out", "*email", "*creationUUID"))
        // Ensure that emailToUserId has no entry for this email
        // Generate a userId and set the emailToUserId entry
        // Jump to the partition via hashed userId
        // Create the user
        .localSelect("$$emailToUserId", Path.key("*email")).out("*userId")
        .ifTrue(new Expr(Ops.IS_NULL, "*userId"),
            Block
                // Generate a userId
                .each(UUID.randomUUID()::toString).out("*userId")
                // Set the emailToUserId entry
                .localTransform("$$emailToUserId", Path.key("*email").termVal("*userId"))
                // TODO will the following code run before the append is acknowledged as required?
                .hashPartition("*userId")
                .localTransform("$$users",
                    Path.key("*userId")
                        .multiPath(
                            Path.key("email").termVal("*email"),
                            Path.key("creationUUID").termVal("*creationUUID")
                        )
                )
        );

  }

  private static void declareVehiclesTopology(Topologies topologies) {
    StreamTopology vehicles = topologies.stream("vehicles");
    vehicles.pstate("$$vehicles", PState.mapSchema(
        String.class, // vehicleId
        PState.fixedKeysSchema(
            "vehicleId", String.class,
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

    // Verify that a vehicle with this id does not exist
    // Add the vehicle to the vehicles pstate (battery = 0, location = (0,0)
    vehicles.source("*vehicleCreate").out("*out")
        .macro(extractJavaFields("*out", "*creationUUID", "*vehicleId"))
        .localTransform("$$vehicles",
            Path.key("*vehicleId")
                // Only performs the write if the current data is null
                .filterPred(Ops.IS_NULL)
                .multiPath(
                    Path.key("vehicleId").termVal("*vehicleId"),
                    Path.key("battery").termVal(0),
                    Path.key("location").termVal(new LatLng(0L, 0L)),
                    Path.key("creationUUID").termVal("*creationUUID")
                )
        );

    vehicles.source("*vehicleUpdate").out("*out")
        .macro(extractJavaFields("*out", "*vehicleId", "*battery", "*location"))
        .localTransform("$$vehicles",
            Path.key("*vehicleId")
                // Only update a vehicle if it exists
                .filterPred(Ops.IS_NOT_NULL)
                .multiPath(
                    Path.key("battery").termVal("*battery"),
                    Path.key("location").termVal("*location")
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
