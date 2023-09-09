package org.example;

import com.rpl.rama.Path;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;
import junit.framework.TestCase;
import org.example.data.LatLng;

import java.util.List;
import java.util.Map;

public class EVClientTest extends TestCase {

  private static final String moduleName = EVModule.class.getName();


  public void testCreateVehicle() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var module = new EVModule();
      var moduleName = module.getClass().getName();
      ipc.launchModule(module, new LaunchConfig(1, 1));
      EVClient client = new EVClient(ipc);

      var vehicles = ipc.clusterPState(moduleName, "$$vehicles");

      // Creating a vehicle should put it in the $$vehicles PState
      var vehicleId = client.createVehicle();
      assertNotNull(vehicles.selectOne(Path.key(vehicleId)));

    }
  }

  private EVClient launchModule(InProcessCluster ipc) {
    var module = new EVModule();
    ipc.launchModule(module, new LaunchConfig(1, 1));
    return new EVClient(ipc);
  }

  public void testUpdateVehicle() throws Exception {
    // Changes to the location and battery of a vehicle should be reflected in the $$vehicles PState
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var vehicles = ipc.clusterPState(moduleName, "$$vehicles");

      var vehicleId = client.createVehicle();
      var location = new LatLng(1L, 2L);
      var battery = 50;
      client.updateVehicle(vehicleId, battery, location);

      Map<String, Object> vehicleMap = vehicles.selectOne(Path.key(vehicleId));
      assertEquals(battery, vehicleMap.get("battery"));
      assertEquals(location, vehicleMap.get("location"));

      // TODO assert that $$vehicleLocationHistory is updated
    }
  }

  public void testCreateAccount() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var users = ipc.clusterPState(moduleName, "$$users");
      var emailToUserId = ipc.clusterPState(moduleName, "$$emailToUserId");

      // Creating an account should put it in the $$users PState
      var validEmail = "test@example.com";
      assertNull(emailToUserId.selectOne(Path.key(validEmail)));
      var created = client.createAccount(validEmail);
      assertTrue(created.isPresent());
      var userId = emailToUserId.selectOne(Path.key(validEmail));
      assertNotNull(userId);
      assertNotNull(users.selectOne(Path.key(userId)));
    }
  }

  public void testCreateAccountInvalidEmail() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var emailToUserId = ipc.clusterPState(moduleName, "$$emailToUserId");

      // Creating an account with an invalid email should fail
      var invalidEmail = "test";
      var created = client.createAccount(invalidEmail);
      assertTrue(created.isEmpty());
      assertNull(emailToUserId.selectOne(Path.key(invalidEmail)));
    }
  }

  public void testGetUserRideHistory() {
    // Begin a ride, update the vehicle location a few times, end the ride, expect a ride history
    // entry with the correct fields
  }


  public void testBeginRide() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var userInRide = ipc.clusterPState(moduleName, "$$userInRide");
      var vehicleRide = ipc.clusterPState(moduleName, "$$vehicleRide");

      // Create a user to ride the vehicle
      var userId = client.createAccount("a@example.com").orElseThrow();

      // Create a vehicle
      var vehicleId = client.createVehicle();
      var startLocation = new LatLng(1L, 2L);

      // update the vehicle location and battery
      client.updateVehicle(vehicleId, 100, startLocation);
      assertTrue(client.beginRide(vehicleId, userId, startLocation).isPresent());
      assertNotNull(vehicleRide.selectOne(Path.key(vehicleId)));
      assertTrue(userInRide.selectOne(Path.key(userId)));

      // Create a new vehicle and attempt to begin a ride
      var vehicleId2 = client.createVehicle();
      client.updateVehicle(vehicleId2, 100, startLocation);
      assertFalse(client.beginRide(vehicleId2, userId, startLocation).isPresent());
      assertNull(vehicleRide.selectOne(Path.key(vehicleId2)));

      // Create a new user and attempt to begin a ride
      var userId2 = client.createAccount("b@example.com").orElseThrow();

      // Attempt to ride a vehicle in an existing ride
      assertFalse(client.beginRide(vehicleId, userId2, startLocation).isPresent());

      // Set battery too low to start
      client.updateVehicle(vehicleId2, 9, startLocation);
      assertFalse(client.beginRide(vehicleId2, userId2, startLocation).isPresent());
      assertNull(vehicleRide.selectOne(Path.key(vehicleId2)));

      // Set battery high enough to start but user too far away
      client.updateVehicle(vehicleId2, 100, new LatLng(3L, 4L));
      assertFalse(client.beginRide(vehicleId2, userId2, startLocation).isPresent());
      assertNull(vehicleRide.selectOne(Path.key(vehicleId2)));
    }
  }

  public void testEndRide() throws Exception {
    try (InProcessCluster ipc = InProcessCluster.create()) {
      var client = launchModule(ipc);
      var vehicleRide = ipc.clusterPState(moduleName, "$$vehicleRide");
      var userRideHistory = ipc.clusterPState(moduleName, "$$userRideHistory");

      // Create a user and vehicle
      var userId = client.createAccount("a@example.com").orElseThrow();
      var vehicleId = client.createVehicle();

      // Attempt to end a ride that doesn't exist (nothing to assert)
      client.endRide(vehicleId, userId);

      // begin a ride
      var startLocation = new LatLng(1L, 2L);
      client.updateVehicle(vehicleId, 100, startLocation);
      var rideId = client.beginRide(vehicleId, userId, startLocation).orElseThrow();
      assertNotNull(vehicleRide.selectOne(Path.key(vehicleId)));

      // Create a second user
      var userId2 = client.createAccount("b@example.com").orElseThrow();

      // Attempt to end the ride as the second user
      client.endRide(vehicleId, userId2);
      // The ride should still be in progress
      assertNotNull(vehicleRide.selectOne(Path.key(vehicleId)));
      // The second user's ride history should be empty
      assertNull(userRideHistory.selectOne(Path.key(userId2)));

      // Update the vehicle location and battery
      var intermediateLocation = new LatLng(3L, 4L);
      client.updateVehicle(vehicleId, 90, intermediateLocation);

      // End the ride
      client.endRide(vehicleId, userId);
      // The ride should no longer be in progress
      assertNull(vehicleRide.selectOne(Path.key(vehicleId)));
      // The user's ride history should be updated
      Map<String, Object> rideHistory = userRideHistory.selectOne(Path.key(userId));
      assertTrue(rideHistory.containsKey(rideId));
      // The ride history should contain three location points
      List<LatLng> route = userRideHistory.selectOne(Path.key(userId, rideId, "route"));
      assertEquals(2, route.size());

      // Attempt to start a new ride
      client.updateVehicle(vehicleId, 100, startLocation);
      client.beginRide(vehicleId, userId2, startLocation).orElseThrow();
      // The ride should be in progress
      assertNotNull(vehicleRide.selectOne(Path.key(vehicleId)));
    }
  }

  public void testGetVehiclesNearLocation() {
  }
}