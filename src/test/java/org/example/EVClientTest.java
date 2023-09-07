package org.example;

import com.rpl.rama.Path;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;
import junit.framework.TestCase;
import org.example.data.LatLng;

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
    }
  }

  public void testCreateAccount() {
  }

  public void testGetUserRideHistory() {
  }

  public void testBeginRide() {
  }

  public void testEndRide() {
  }
}