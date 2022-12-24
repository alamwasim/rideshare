import heapq
from collections import defaultdict, deque
from itertools import count

import pytest


class User(object):
    def __init__(self, name: str, age: int, sex: str):
        self.name = name
        self.age = age
        self.sex = sex
        self.vehicle = []
        self.active_offered_ride = set()
        self.active_selected_ride = set()
        self.offered = 0
        self.taken = 0

    def ride_offered(self, ride_id: int):
        self.active_offered_ride.add(ride_id)
        self.offered += 1

    def ride_taken(self, ride_id: int):
        self.active_selected_ride.add(ride_id)
        self.taken += 1


class Vehicle(object):
    def __init__(self, name: str, vehicle: str, vehicle_no: str, vehicle_id=None):
        self.name = name
        self.vehicle = vehicle
        self.vehicle_no = vehicle_no
        self.active_ride = None
        if vehicle_id: self.vehicle_id = vehicle_id

    def update_active_ride(self, ride_id: str):
        self.active_ride = ride_id

    @staticmethod
    def get_vehicle_id(name, vehicle_model, vehicle_no):
        return '_'.join([name, vehicle_model, vehicle_no])


class Ride(object):
    counter = count(1)

    def __init__(self, user: User, origin: str, destination: str, available_seat: int, vehicle: Vehicle):
        self.ride_id = next(Ride.counter)
        self.driver = user
        self.origin = origin
        self.destination = destination
        self.available_seat = available_seat
        self.vehicle = vehicle
        self.active_passenger = []

    def ride_selected(self, passenger, seats_booked):
        if seats_booked <= self.available_seat:
            self.available_seat -= seats_booked
            self.active_passenger.append((passenger, seats_booked))
        else:
            raise ValueError("Booked seats can't be greater than avaialble seats")


class RidePriorityQueue:
    def __init__(self):
        self.rideMap = defaultdict(list)  # {str : [],}
        self.total_rides = 0
        self.total_available_seats = 0

    def push_offered_ride(self, ride: Ride):
        priority = ride.available_seat
        value = ride.ride_id
        heapq.heappush(self.rideMap[ride.vehicle.vehicle], (-priority, value))
        self.total_rides += 1
        self.total_available_seats += priority

    def select_ride(self, passenger: str, seats: int, most_vacant: bool, prefered_vehicle: str) -> int:
        ride_id = None

        if self.total_available_seats < seats:
            return -1

        if most_vacant and not prefered_vehicle:
            prefered_vehicle, elem = min(self.rideMap.items(), key=lambda x: x[1][0][
                0])  # TODO: TC can be improved by using heapdict from O(k) to O(log(k))
            if abs(elem[0][0]) < seats: prefered_vehicle = None
        if prefered_vehicle and prefered_vehicle in self.rideMap:
            if len(self.rideMap[prefered_vehicle]) > 0 and abs(self.rideMap[prefered_vehicle][0][0]) >= seats:
                available_seats, ride_id = self.rideMap[prefered_vehicle][0]
                heapq.heapreplace(self.rideMap[prefered_vehicle], (-(abs(available_seats) - seats), ride_id))
        else:
            print('Prefered Vehicle not available')
        if ride_id:
            ride = RideShare.ride_db.get(ride_id)
            ride.ride_selected(passenger, seats)
            self.total_available_seats -= seats
        return ride_id

    def end_ride(self, ride_id):
        ride = RideShare.ride_db.get(ride_id)
        if ride and len(ride.active_passenger) == 0:
            self.rideMap[ride.vehicle.vehicle].remove((-ride.available_seat, ride.ride_id))
            if len(self.rideMap[ride.vehicle.vehicle]) == 0:
                del self.rideMap[ride.vehicle.vehicle]
            else:
                heapq.heapify(self.rideMap[
                                  ride.vehicle.vehicle])  # TODO: Can be improved to O(log(n)) by using Mapped Heap Queue but space complexity will get doubled
        else:
            raise ValueError("Ride not found or can't be ended due to active offer")


class RideShare(object):
    user_db = {}  # {int:User,}
    vehicle_db = {}  # {str:Vehicle,}
    ride_db = {}  # {int:Ride,}
    ride_map_db = defaultdict(lambda: defaultdict(RidePriorityQueue))  # {str: {str: RidePriorityQueue},}

    def add_user(self, name: str, sex: str, age: int):
        if name not in RideShare.user_db:
            RideShare.user_db[name] = User(name, age, sex)
        else:
            print('User Already Exists!!')

    def add_vehicle(self, name, vehicle_model, vehicle_no):
        if name in RideShare.user_db:
            vehicle_id = Vehicle.get_vehicle_id(name, vehicle_model, vehicle_no)
            if vehicle_id not in RideShare.vehicle_db:
                vehicle = Vehicle(name, vehicle_model, vehicle_no, vehicle_id=vehicle_id)
                RideShare.vehicle_db[vehicle_id] = vehicle
                RideShare.user_db[name].vehicle.append(vehicle)
            else:
                print('Particular vehicle already added')
        else:
            print('User not registered. Please first add user.')

    def offer_ride(self, name: str, origin: str, seats: int, vehicle_model: str, vehicle_no: str, destination: str):
        """
        Offered ride are given new ride_id based on counter. the data is maintained in ride_map_db. The value of ride_map_db is RidePriorityQueue class.
        This class maintains the map with key as vehicle-model and values as priority queue of ride_id with available seats as priority.
        TC of pushing new ride will be O(log(k)) where k is the number of rides for particular vehicle-model in that source to destination trip.
        :param name:
        :param origin:
        :param seats:
        :param vehicle_model:
        :param vehicle_no:
        :param destination:
        :return:
        """
        user = RideShare.user_db.get(name)
        if not user: raise ValueError('User Not Exists')

        vehicle_id = Vehicle.get_vehicle_id(name, vehicle_model, vehicle_no)
        vehicle = RideShare.vehicle_db.get(vehicle_id)
        if not vehicle: raise ValueError('Vehicle Not Registered')
        if vehicle.active_ride is not None: raise ValueError('Ride already offered with this vehicle')

        ride = Ride(user, origin, destination, seats, vehicle)
        RideShare.ride_db[ride.ride_id] = ride
        RideShare.ride_map_db[origin][destination].push_offered_ride(ride)
        user.ride_offered(ride.ride_id)
        vehicle.update_active_ride(ride.ride_id)
        return ride.ride_id

    def select_ride(self, name, origin, destination, seats, most_vacant=True, prefered_vehicle=None) -> [int]:
        """
        Return list of ride_ids based on selection criteria.
        TC O(log(k)) where available seats for selected ride_id are updated and priority queue is maintained.
        For most_vacant selection criteria, TC will be O(m) + O(log(k)) where m is unique vehicle-model in the given source to
        destination trip. Since m will always be very small number, so it will become constant. Else this can improved using heapDict.
        In Production this can be split into functions where selected ride_id can be returned in O(1) and asynchronously
        priority queue can be updated.
        :param name:
        :param origin:
        :param destination:
        :param seats:
        :param most_vacant:
        :param prefered_vehicle:
        :return:
        """
        user = RideShare.user_db.get(name)
        if not user: raise ValueError('User Not Exists')
        if not 0 < seats < 3: raise ValueError('Selected seats should be either 1 or 2')
        if RideShare.ride_map_db.get(origin):
            if RideShare.ride_map_db[origin].get(destination):
                selected_ride_id = RideShare.ride_map_db[origin][destination].select_ride(name, seats, most_vacant,
                                                                                          prefered_vehicle)
                if selected_ride_id:
                    user.ride_taken(selected_ride_id)
                    return [selected_ride_id]
            else:
                print('No direct ride to destination')
        else:
            print('No ride from the origin')
        return [-1]

    def end_ride(self, ride_id):
        """
        End the ride for ride_id if there are no active passengers for that ride.
        TC O(k) where k is the number of rides present for that particular vehicle-model in that given origin to destination trip.
        TC can be further improved to O(log(k)) by using MappedHeapQueue. This is not done here for sake of simplicity.
        :param ride_id:
        :return:
        """
        if ride_id in RideShare.ride_db:
            ride = RideShare.ride_db[ride_id]
            if len(ride.active_passenger) == 0:
                RideShare.ride_map_db[ride.origin][ride.destination].end_ride(ride_id)
                ride.driver.active_offered_ride.remove(ride_id)
                del RideShare.ride_db[ride_id]
                return True
            else:
                raise ValueError("Active offered rides found. Rides can't be deleted")
        else:
            print('Ride not found')
        return False

    def print_ride_stats(self):
        out = []
        for user, val in RideShare.user_db.items():
            out.append(f"{user}: {val.taken} Taken, {val.offered} Offered")
        return out

    def find_multiple_rides(self, origin, destination, seats):
        q = deque()
        q.append((origin, origin))
        visited = {origin}
        all_possible = []
        while q:
            node, path = q.popleft()
            neighbors = [d for d, v in RideShare.ride_map_db.get(node, {}).items() if v.total_available_seats >= seats]
            for nbr in neighbors:
                q.append((nbr, path + '-->' + nbr))
                visited.add(nbr)
                if nbr == destination:
                    all_possible.append(path + '-->' + nbr)  ##TODO : Break from loop upon meeting first conditions
        print('all_possible_path', all_possible)
        return all_possible[0].split('-->') if all_possible else ''

    def select_multiple_rides(self, name, origin, destination, seats):
        selected_ride_id = []
        possible_path = self.find_multiple_rides(origin, destination, seats)
        print('selected_possible_path', possible_path)
        for i in range(len(possible_path) - 1):
            selected_ride_id.extend(self.select_ride(name, possible_path[i], possible_path[i + 1], seats))
        return selected_ride_id


def test():
    rideshare = RideShare()

    ##Onboard 6 users
    rideshare.add_user("Rohan", "M", 36)
    rideshare.add_vehicle("Rohan", "Swift", "KA-01-12345")

    rideshare.add_user("Shashank", "M", 29)
    rideshare.add_vehicle("Shashank", "Baleno", "TS-05-62395")

    rideshare.add_user("Nandini", "F", 29)

    rideshare.add_user("Shipra", "F", 27)
    rideshare.add_vehicle("Shipra", "Polo", "KA-05-41491")
    rideshare.add_vehicle("Shipra", "Activa", "KA-12-12332")

    rideshare.add_user("Gaurav", "M", 29)

    rideshare.add_user("Rahul", "M", 35)
    rideshare.add_vehicle("Rahul", "XUV", "KA-05-1234")

    ##Offer 5 rides by 4 users
    assert rideshare.offer_ride("Rohan", origin="Hyderabad", seats=1, vehicle_model="Swift", vehicle_no="KA-01-12345",
                                destination="Bangalore") == 1
    assert rideshare.offer_ride("Shipra", origin="Bangalore", seats=1, vehicle_model="Activa", vehicle_no="KA-12-12332",
                                destination="Mysore") == 2
    assert rideshare.offer_ride("Shipra", origin="Bangalore", seats=2, vehicle_model="Polo", vehicle_no="KA-05-41491",
                                destination="Mysore") == 3
    assert rideshare.offer_ride("Shashank", origin="Hyderabad", seats=2, vehicle_model="Baleno",
                                vehicle_no="TS-05-62395",
                                destination="Bangalore") == 4
    assert rideshare.offer_ride("Rahul", origin="Hyderabad", seats=5, vehicle_model="XUV", vehicle_no="KA-05-1234",
                                destination="Bangalore") == 5
    with pytest.raises(ValueError):
        rideshare.offer_ride("Rohan", origin="Bangalore", seats=1, vehicle_model="Swift", vehicle_no="KA-01-12345",
                             destination="Pune")

    ##Select 5 rides by 4 users
    assert rideshare.select_ride("Nandini", origin="Bangalore", destination="Mysore", seats=1, most_vacant=True) == [3]

    assert rideshare.select_ride("Gaurav", origin="Bangalore", destination="Mysore", seats=1,
                                 prefered_vehicle="Activa") == [2]

    assert rideshare.select_ride("Shashank", origin="Mumbai", destination="Bangalore", seats=1, most_vacant=True) == [
        -1]

    assert rideshare.select_ride("Rohan", origin="Hyderabad", destination="Bangalore", seats=1,
                                 prefered_vehicle="Baleno") == [4]

    assert rideshare.select_ride("Shashank", origin="Hyderabad", destination="Bangalore", seats=1,
                                 prefered_vehicle="Polo") == [-1]

    ##print Stats
    out = ['Rohan: 1 Taken, 1 Offered',
           'Shashank: 0 Taken, 1 Offered',
           'Nandini: 1 Taken, 0 Offered',
           'Shipra: 0 Taken, 2 Offered',
           'Gaurav: 1 Taken, 0 Offered',
           'Rahul: 0 Taken, 1 Offered']
    assert rideshare.print_ride_stats() == out

    ##End Rides
    assert rideshare.end_ride(1) == True
    with pytest.raises(ValueError): rideshare.end_ride(2)
    with pytest.raises(ValueError): rideshare.end_ride(3)
    with pytest.raises(ValueError): rideshare.end_ride(4)
    assert rideshare.end_ride(5) == True
    assert rideshare.end_ride(6) == False

    ##Selaect Multiple Rides if direct rides not available
    rideshare.add_vehicle("Rohan", "Swift", "TS-05-62396")
    rideshare.add_vehicle("Rohan", "Swift", "TS-05-62397")
    rideshare.add_vehicle("Rohan", "Swift", "TS-05-62398")
    rideshare.add_vehicle("Rohan", "Swift", "TS-05-62399")
    rideshare.add_vehicle("Rohan", "Swift", "TS-05-62390")
    rideshare.add_vehicle("Rohan", "Swift", "TS-05-62391")
    rideshare.add_vehicle("Rohan", "Swift", "TS-05-62392")
    rideshare.add_vehicle("Rohan", "Swift", "TS-05-62393")

    rideshare.offer_ride("Rohan", origin="Hyderabad", seats=1, vehicle_model="Swift", vehicle_no="TS-05-62396",
                         destination="Bangalore")
    rideshare.offer_ride("Rohan", origin="Bangalore", seats=1, vehicle_model="Swift", vehicle_no="TS-05-62397",
                         destination="Pune")
    rideshare.offer_ride("Rohan", origin="Pune", seats=1, vehicle_model="Swift", vehicle_no="TS-05-62398",
                         destination="Mysore")
    rideshare.offer_ride("Rohan", origin="Hyderabad", seats=1, vehicle_model="Swift", vehicle_no="TS-05-62399",
                         destination="Chennai")
    rideshare.offer_ride("Rohan", origin="Chennai", seats=1, vehicle_model="Swift", vehicle_no="TS-05-62390",
                         destination="Bangalore")
    rideshare.offer_ride("Rohan", origin="Chennai", seats=1, vehicle_model="Swift", vehicle_no="TS-05-62391",
                         destination="Mysore")
    rideshare.offer_ride("Rohan", origin="Bangalore", seats=1, vehicle_model="Swift", vehicle_no="TS-05-62392",
                         destination="Ootie")
    rideshare.offer_ride("Rohan", origin="Ootie", seats=1, vehicle_model="Swift", vehicle_no="TS-05-62393",
                         destination="Mysore")

    assert rideshare.select_multiple_rides('Shipra', 'Hyderabad', 'Mysore', 1) == [4, 3]
    assert rideshare.select_multiple_rides('Gaurav', 'Hyderabad', 'Goa', 1) == []


if __name__ == '__main__':
    test()
