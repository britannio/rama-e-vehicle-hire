# rama-e-vehicle-hire

I created this project to practice using the Rama platform (https://redplanetlabs.com/learn-rama).

> In development for 10 years, Rama is a new programming platform that reduces the cost of building scalable backends by 100x. Rama integrates and generalizes every aspect of data ingestion, processing, indexing, and querying. Rama is deployed as a cluster and programmed with a pure Java API.

The project is a back-end for a hypothetical e-vehicle platform whereby riders can create an account, locate nearby vehicles, and begin rides.

By virtue of using Rama, the back-end is horizontally scalable from the get-go and some of the more complex cross-task logic is tested with multiple threads to demonstrate this.


Global Objects:
- vehicleLocationTree: Stores a `GlobalKDTree` on each task ('partition'). The KDTree tracks the location of vehicles. Querying across all KDTrees is done to gather the nearest 50 vehicles.

Depots:
- vehicleCreate: Create a new vehicle
- vehicleUpdate: Update the location and battery of a vehicle
- userRegistration: Create a new user
- ride: Begin/end a ride


PStates:
- user: Maps a userId to an email and other user data.
- emailToUserId: Maps an email to a user id.
- vehicle: Maps a vehicle id to all vehicle properties (battery, location)
- vehicleLocationHistory: Maps a vehicle id to a subindexed map of timestamps to locations. Effectively a sorted location history.
- vehicleRide: Maps a vehicle id to ride data if the vehicle is currently in a ride.
- userRideHistory: Maps a user id to a map of rides keyed by ride id.

Queries:
- nearestVehicles: Get the 50 nearest vehicles to a given location.

