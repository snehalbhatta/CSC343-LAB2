"""CSC343 Assignment 2

=== CSC343 Winter 2023 ===
Department of Computer Science,
University of Toronto

This code is provided solely for the personal and private use of
students taking the CSC343 course at the University of Toronto.
Copying for purposes other than this use is expressly prohibited.
All forms of distribution of this code, whether as given or with
any changes, are expressly prohibited.

Authors: Danny Heap, Marina Tawfik, and Jacqueline Smith

All of the files in this directory and all subdirectories are:
Copyright (c) 2023 Danny Heap and Jacqueline Smith

=== Module Description ===

This file contains the WasteWrangler class and some simple testing functions.
"""

import datetime as dt
import psycopg2 as pg
import psycopg2.extensions as pg_ext
import psycopg2.extras as pg_extras
from typing import Optional, TextIO

class WasteWrangler:
    """A class that can work with data conforming to the schema in
    waste_wrangler_schema.ddl.

    === Instance Attributes ===
    connection: connection to a PostgreSQL database of a waste management
    service.

    Representation invariants:
    - The database to which connection is established conforms to the schema
      in waste_wrangler_schema.ddl.
    """
    connection: Optional[pg_ext.connection]

    def __init__(self) -> None:
        """Initialize this WasteWrangler instance, with no database connection
        yet.
        """
        self.connection = None

    def connect(self, dbname: str, username: str, password: str) -> bool:
        """Establish a connection to the database <dbname> using the
        username <username> and password <password>, and assign it to the
        instance attribute <connection>. In addition, set the search path
        to waste_wrangler.

        Return True if the connection was made successfully, False otherwise.
        I.e., do NOT throw an error if making the connection fails.

        >>> ww = WasteWrangler()
        >>> ww.connect("csc343h-marinat", "marinat", "")
        True
        >>> # In this example, the connection cannot be made.
        >>> ww.connect("invalid", "nonsense", "incorrect")
        False
        """
        try:
            self.connection = pg.connect(
                dbname=dbname, user=username, password=password,
                options="-c search_path=waste_wrangler"
            )
            return True
        except pg.Error:
            return False

    def disconnect(self) -> bool:
        """Close this WasteWrangler's connection to the database.

        Return True if closing the connection was successful, False otherwise.
        I.e., do NOT throw an error if closing the connection failed.

        >>> ww = WasteWrangler()
        >>> ww.connect("csc343h-marinat", "marinat", "")
        True
        >>> ww.disconnect()
        True
        """
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
            return True
        except pg.Error:
            return False

    def get_trip_end_time(self,rid: int, start_time: dt.datetime) -> dt.datetime:
        cursor = self.connection.cursor()
        cursor.execute("select length from Route where rID = %s",[rid])
        trip_distance = cursor.fetchone()
        print("trip distance is",trip_distance[0]);
        trip_time = trip_distance[0]/5
        end_time = start_time + dt.timedelta(hours = trip_time)
        if cursor and not cursor.closed:
            cursor.close()
        return end_time
    def get_facility(self,rid: int) -> int:
        #picking the correct faility
        cursor = self.connection.cursor()
        #view clean up
        cursor.execute("DROP VIEW IF EXISTS wasteRoute CASCADE")
        cursor.execute("DROP VIEW IF EXISTS AvlbFac CASCADE")

        cursor.execute("create view wasteRoute as select wasteType from Route where rID=%s",[rid])
        cursor.execute("create view AvlbFac as select fID from Facility,wasteRoute where Facility.wasteType = wasteRoute.wasteType order by fID")
        cursor.execute("select * from AvlbFac")
        selected_facility = cursor.fetchone()
        if cursor and not cursor.closed:
            cursor.close()
        return selected_facility

    def get_available_emp_pair(self,rid: int,start_time: dt.datetime,tid: int,free_constraint:bool):
        #free constraint =0 implies driver pair must be free 30 min before and 30 minutes after the scheduled trip time
        #free constraint =1 implies driver pair must be free all day - should have no trips that day
        #order by hire date,eID, atleast one should be able to drive a truck
        cursor = self.connection.cursor()
        #view clean up
        cursor.execute("DROP VIEW IF EXISTS UnavlbDriverPairs CASCADE")
        cursor.execute("DROP VIEW IF EXISTS UnavlbDrivers CASCADE")
        cursor.execute("DROP VIEW IF EXISTS FreeDrivers  CASCADE")
        cursor.execute("DROP VIEW IF EXISTS TypeFromTid CASCADE")
        cursor.execute("DROP VIEW IF EXISTS AvlbDrivers CASCADE")
        cursor.execute("DROP VIEW IF EXISTS AvlbDriversMatching CASCADE")
        cursor.execute("DROP VIEW IF EXISTS DriversLeft CASCADE")
                                            

        if(free_constraint  == 1): #for schedule_trips function
            #Selecting employees having free time- no trips constraint
            cursor.execute("create view UnavlbDriverPairs as select eID1,eID2 from Trip where date(ttime) = %s",[start_time.date()])
            cursor.execute("create view UnavlbDrivers as (select eID1 from UnavlbDriverPairs) UNION (select eID2 from UnavlbDriverPairs)")
            print("prinitng all unavalb drivers")
            cursor.execute("select * from UnavlbDrivers")
            for row in cursor:
                print(row)
            cursor.execute("create view FreeDrivers as (select eID from Driver) EXCEPT (select * from UnavlbDrivers)")   
            cursor.execute("select * from FreeDrivers")
            print("printing free drivers") 
            for row in cursor:
                print(row)
            # get the truck type of the tid
            cursor.execute("create view TypeFromTid as select Truck.truckType from Truck join TruckType on Truck.truckType = TruckType.truckType where tID = %s",[tid])
            cursor.execute("select * from TypeFromTid")
            truck_type = cursor.fetchone()
            print("the truck type from id is ",cursor.fetchone())
            cursor.execute("create view AvlbDrivers as select FreeDrivers.eID as eID,hireDate,truckType from FreeDrivers join Employee on FreeDrivers.eID=Employee.eID join Driver on Employee.eID= Driver.eID order by hireDate,eID")
            cursor.execute("create view AvlbDriversMatching as select * from AvlbDrivers where truckType =%s order by hireDate,eID",[truck_type])          
            cursor.execute("select * from AvlbDrivers")
            print("available driver",cursor.fetchall())
            cursor.execute("select * from AvlbDriversMatching")
            selected_driver_one = cursor.fetchone()
            if(selected_driver_one is not None):
                selected_driver_eID1 = selected_driver_one[0]
                print("the first driver is" ,selected_driver_eID1)
            else:
                selected_driver_eID1 = None
            cursor.execute("create view DriversLeft as select * from AvlbDrivers where eID != %s order by hireDate,eID",[selected_driver_eID1])
            cursor.execute("select * from DriversLeft")
            selected_driver_two  = cursor.fetchone()
            if(selected_driver_two is not None):
                selected_driver_eID2 = selected_driver_two[0]
                print("the second driver is" ,selected_driver_eID2)
            else:
                selected_driver_eID2 = None
            
            if cursor and not cursor.closed:
               cursor.close()
            return selected_driver_eID1,selected_driver_eID2
        else: 
            if cursor and not cursor.closed:
               cursor.close()
            return None,None 

    def get_emp_pair_for_trip(self,rid: int, start_time_before: dt.datetime,end_time_after: dt.datetime,tid: int):
        cursor = self.connection.cursor()
        #view clean up
        cursor.execute("DROP VIEW IF EXISTS UnavlbDriverPairs CASCADE")
        cursor.execute("DROP VIEW IF EXISTS UnavlbDrivers CASCADE")
        cursor.execute("DROP VIEW IF EXISTS FreeDrivers  CASCADE")
        cursor.execute("DROP VIEW IF EXISTS TypeFromTid CASCADE")
        cursor.execute("DROP VIEW IF EXISTS AvlbDrivers CASCADE")
        cursor.execute("DROP VIEW IF EXISTS AvlbDriversMatching CASCADE")
        cursor.execute("DROP VIEW IF EXISTS DriversLeft CASCADE")



        cursor.execute("create view UnavlbDriverPairs as select eID1,eID2 from Trip where (ttime >= %s and ttime <= %s)",    
                            [start_time_before,end_time_after])
        cursor.execute("create view UnavlbDrivers as (select eID1 from UnavlbDriverPairs) UNION (select eID2 from UnavlbDriverPairs)")
        print("prinitng all unavalb drivers")
        cursor.execute("select * from UnavlbDrivers")
        for row in cursor:
            print(row)
        cursor.execute("create view FreeDrivers as (select eID from Driver) EXCEPT (select * from UnavlbDrivers)")   
        cursor.execute("select * from FreeDrivers")
        print("printing free drivers") 
        for row in cursor:
            print(row)
        # get the truck type of the tid
        cursor.execute("create view TypeFromTid as select Truck.truckType from Truck join TruckType on Truck.truckType = TruckType.truckType where tID = %s",[tid])
        cursor.execute("select * from TypeFromTid")
        truck_type = cursor.fetchone()
        print("the truck type from id is ",cursor.fetchone())
        cursor.execute("create view AvlbDrivers as select FreeDrivers.eID as eID,hireDate,truckType from FreeDrivers join Employee on FreeDrivers.eID=Employee.eID join Driver on Employee.eID= Driver.eID order by hireDate,eID")
        cursor.execute("create view AvlbDriversMatching as select * from AvlbDrivers where truckType =%s  order by hireDate,eID",[truck_type])          
        cursor.execute("select * from AvlbDrivers")
        print("available driver",cursor.fetchall())
        cursor.execute("select * from AvlbDriversMatching")
        selected_driver_one = cursor.fetchone()
        if(selected_driver_one is not None):
            selected_driver_eID1 = selected_driver_one[0]
            print("the first driver is" ,selected_driver_eID1)
        else:
            selected_driver_eID1 = None
        cursor.execute("create view DriversLeft as select * from AvlbDrivers where eID != %s order by hireDate,eID",[selected_driver_eID1])
        cursor.execute("select * from DriversLeft")
        selected_driver_two  = cursor.fetchone()
        if(selected_driver_two is not None):
            selected_driver_eID2 = selected_driver_two[0]
            print("the second driver is" ,selected_driver_eID2)
        else:
            selected_driver_eID2 = None
            
        if cursor and not cursor.closed:
           cursor.close()
        return selected_driver_eID1,selected_driver_eID2

    def choose_day_tech(self,tid: int,fromdate: dt.date):
        #clean up views
        cursor = self.connection.cursor()
        cursor2 = self.connection.cursor()
        date_not_found = True
        starting_date = fromdate + dt.timedelta(days=1)
        tech_ID = -1 # intialized
        while(date_not_found):
            cursor.execute("DROP VIEW IF EXISTS busyTechs CASCADE")
            cursor.execute("DROP VIEW IF EXISTS freeTechs CASCADE")
            #check truck not scheduled for trip
            cursor.execute("select * from Trip where tID=%s and date(ttime)=%s",[tid,starting_date])
            #check truck not scheduled for main
            cursor2.execute("select * from Maintenance where tID=%s and mDate=%s",[tid,starting_date])
            if(cursor.rowcount!=0 or (cursor2.rowcount!=0)):
                starting_date = starting_date + dt.timedelta(days=1)
                continue #truck not available for maint on this day
            else:
                #check techn available
                cursor.execute("create view busyTechs as select eID from Maintenance where mDate=%s and tID!=%s",[starting_date,tid])
                cursor.execute("create view freeTechs as (select eID from Technician) EXCEPT (select * from busyTechs) order by eID")
                cursor.execute("select * from freeTechs")
                if(cursor.rowcount == 0):
                    #no tech found for this date
                    starting_date = starting_date + dt.timedelta(days=1)
                    continue
                else:
                    #pick the first one as we have ordered by eID
                    tech_ID = cursor.fetchone()
                    date_not_found = False
        if cursor and not cursor.closed:
           cursor.close()
        print("returning the date: and tech: ",starting_date,tech_ID)
        return starting_date,tech_ID


    def insert_Trip(self,rid: int,tid: int,ttime: dt.datetime,volume: float,eid1: int,eid2: int,fid: int ) -> int:
        cursor = self.connection.cursor()
        try:
            cursor.execute("Insert Into Trip values (%s, %s, %s, %s, %s, %s, %s);", (rid,tid,ttime,volume,eid1,eid2,fid))
        except pg.Error as ex:
            # An error occurred so we need to rollback the connection.
            self.connection.rollback()
            insertion_success = False
            print("insertion success set to",insertion_success)
        else:
            # No error has occured.
            self.connection.commit()
            insertion_success = True
            print("insertion success set to",insertion_success)
        finally:
            # This code will always execute. 
            if cursor and not cursor.closed:
               cursor.close()
            return insertion_success
        
    def insert_Maint(self,tid: int,eid: int,mDate:dt.date) -> int:
        cursor = self.connection.cursor()
        try:
            cursor.execute("Insert Into Maintenance values (%s, %s, %s);", (tid,eid,mDate))
        except pg.Error as ex:
            # An error occurred so we need to rollback the connection.
            self.connection.rollback()
            insertion_success = False
            print("insertion into maint success set to",insertion_success)
        else:
            # No error has occured.
            self.connection.commit()
            insertion_success = True
            print("insertion into maint success set to",insertion_success)
        finally:
            # This code will always execute. 
            if cursor and not cursor.closed:
               cursor.close()
            return insertion_success
       

    def schedule_trip(self, rid: int, time: dt.datetime) -> bool:
        """Schedule a truck and two employees to the route identified
        with <rid> at the given time stamp <time> to pick up an
        unknown volume of waste, and deliver it to the appropriate facility.

        The employees and truck selected for this trip must be available:
            * They can NOT be scheduled for a different trip from 30 minutes
              of the expected start until 30 minutes after the end time of this
              trip.
            * The truck can NOT be scheduled for maintenance on the same day.

        The end time of a trip can be computed by assuming that all trucks
        travel at an average of 5 kph.

        From the available trucks, pick a truck that can carry the same
        waste type as <rid> and give priority based on larger capacity and
        use the ascending order of ids to break ties.

        From the available employees, give preference based on hireDate
        (employees who have the most experience get priority), and order by
        ascending order of ids in case of ties, such that at least one
        employee can drive the truck type of the selected truck.

        Pick a facility that has the same waste type a <rid> and select the one
        with the lowest fID.

        Return True iff a trip has been scheduled successfully for the given
            route.
        This method should NOT throw an error i.e. if scheduling fails, the
        method should simply return False.

        No changes should be made to the database if scheduling the trip fails.

        Scheduling fails i.e., the method returns False, if any of the following
        is true:
            * If rid is an invalid route ID.
            * If no appropriate truck, drivers or facility can be found.
            * If a trip has already been scheduled for <rid> on the same day
              as <time> (that encompasses the exact same time as <time>).
            * If the trip can't be scheduled within working hours i.e., between
              8:00-16:00.

        While a realistic use case will provide a <time> in the near future, our
        tests could use any valid value for <time>.
        """
        try:
            # TODO: implement this method
            #Step One: check for valid route ID
            cursor = self.connection.cursor()
            #clean up all the temporary view names
            cursor.execute("DROP VIEW IF EXISTS avlbtrucks CASCADE")
            cursor.execute("DROP VIEW IF EXISTS wasteRoute CASCADE")
            cursor.execute("DROP VIEW IF EXISTS unavlbtrucks CASCADE")
            cursor.execute("DROP VIEW IF EXISTS freetrucks CASCADE")
            cursor.execute("DROP VIEW IF EXISTS AvlbTrucksTypes CASCADE")
            cursor.execute("DROP VIEW IF EXISTS TruckToSelect CASCADE")


            cursor.execute("select * from Route where rID = %s",[rid])
            if(cursor.rowcount == 0):
               return False;
            #Step Two : check if a trip at the same time exsists
            cursor.execute("select * from trip")
            print("the trips relation is ", cursor.fetchall())
            cursor.execute("select * from  Trip where rID = %s AND date(ttime) = %s",[rid,time.date()])
            print("cursor returned",cursor.fetchall())
            if(cursor.rowcount !=0):
               return False;
            #Step Three : check time constraints for start and end time of trips.
            print("start time before is",time - dt.timedelta(minutes=30))
            start_time_before = time + dt.timedelta(minutes=30)
            end_time = self.get_trip_end_time(rid,time)
            end_time_after = end_time + dt.timedelta(minutes=30) 
            print("end time after is ",end_time_after)
            if(time.time() < dt.time(8,0,0) or end_time.time() > dt.time(16,0,0)):
               return False;
            #Finding available trucks
            cursor.execute("create view UnavlbTrucks as select tID from Trip where (ttime >= %s and ttime <= %s)",
                            [start_time_before,end_time_after])
            cursor.execute("select * from UnavlbTrucks")
            print("prinitng unavalb trucks")
            for row in cursor:
                print(row)                        
            cursor.execute("create view FreeTrucks as (select tID from Truck) EXCEPT (select * from UnavlbTrucks)")
            cursor.execute("select * from FreeTrucks")
            print("printing free trucks")
            for row in cursor:
                print(row)
            cursor.execute("create view AvlbTrucks as (select * from FreeTrucks) EXCEPT (select tID from Maintenance where mDate = %s)",[time.date()])
            #Matching waste types
            cursor.execute("create view wasteRoute as select wasteType from Route where rID=%s",[rid])
            cursor.execute("create view AvlbTrucksTypes as select AvlbTrucks.tID,TruckType.truckType,capacity from AvlbTrucks join Truck on AvlbTrucks.tID=Truck.tID join  TruckType on  TruckType.truckType= Truck.truckType join  wasteRoute on wasteRoute.wasteType=TruckType.wasteType")
            cursor.execute("create view TruckToSelect as select tID,truckType from AvlbTrucksTypes order by capacity DESC, tID")
            cursor.execute("select * from TruckToSelect")
            selected_truck = cursor.fetchone()
            if(selected_truck is None):
               truck_found = False
            else:
               selected_truck_id = selected_truck[0]
               selected_truck_type = selected_truck[1]                         
               print("the selected truck is",selected_truck_id,selected_truck_type)
               truck_found = True
            #Selecting employees having free time- no trips constraint
            (eID1,eID2)=self.get_emp_pair_for_trip(rid,start_time_before,end_time_after,selected_truck_id)
            if(eID1 is None or eID2 is None):
                driver_found = False
            else:
                driver_found = True
            #ordering the eids for constraints in trip schema
            selected_driver_eID1  = max(eID1,eID2)
            selected_driver_eID2 = min(eID1,eID2)
     
            #picking the correct faility
            selected_facility = self.get_facility(rid)
            if(selected_facility is None):
                facility_found=False
            else:
               facility_found = True
            #checking if all values availbale for insertion
            if(facility_found and driver_found and truck_found):
               insert_success = self.insert_Trip(rid,selected_truck_id,time,None,selected_driver_eID1,selected_driver_eID2,selected_facility)
               if cursor and not cursor.closed:
                  cursor.close()
               return insert_success
            else:
               if cursor and not cursor.closed:
                  cursor.close()
               return False
            
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            #raise ex
            return False

    def schedule_trips(self, tid: int, date: dt.date) -> int:
        """Schedule the truck identified with <tid> for trips on <date> using
        the following approach:

            1. Find routes not already scheduled for <date>, for which <tid>
               is able to carry the waste type. Schedule these by ascending
               order of rIDs.

            2. Starting from 8 a.m., find the earliest available pair
               of drivers who are available all day. Give preference
               based on hireDate (employees who have the most
               experience get priority), and break ties by choosing
               the lower eID, such that at least one employee can
               drive the truck type of <tid>.

               The facility for the trip is the one with the lowest fID that can
               handle the waste type of the route.

               The volume for the scheduled trip should be null.

            3. Continue scheduling, making sure to leave 30 minutes between
               the end of one trip and the start of the next, using the
               assumption that <tid> will travel an average of 5 kph.
               Make sure that the last trip will not end after 4 p.m.

        Return the number of trips that were scheduled successfully.

        Your method should NOT raise an error.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.
        """
        # TODO: implement this method
        #clean up all the temporary view names
        cursor = self.connection.cursor()      
        cursor.execute("DROP VIEW IF EXISTS AllRoutes CASCADE")
        cursor.execute("DROP VIEW IF EXISTS RoutesSch CASCADE")
        cursor.execute("DROP VIEW IF EXISTS RoutesNotSchCompt CASCADE")
        cursor.execute("DROP VIEW IF EXISTS RoutesNotSch CASCADE")
        cursor.execute("DROP VIEW IF EXISTS RoutesNotSchwaste CASCADE")
        cursor.execute("DROP VIEW IF EXISTS Compatwaste CASCADE")
        #Step One : Get routes that we want to schedule on
        cursor.execute("create view AllRoutes as select rID from Route")
        cursor.execute("create view RoutesSch as select rID from Trip where date(ttime) = %s",[date])
        cursor.execute("create view RoutesNotSch as (select * from AllRoutes) EXCEPT (select * from RoutesSch)")
        cursor.execute("create view RoutesNotSchwaste as select RoutesNotSch.rID,wasteType from RoutesNotSch join Route on Route.rID = RoutesNotSch.rID")
        cursor.execute("create view Compatwaste as select wasteType from TruckType join Truck on TruckType.truckType = Truck.truckType where tID = %s",[tid])
        cursor.execute("select * from Compatwaste")
        print("compatible waste is ",cursor.fetchall())
        cursor.execute("create view RoutesNotSchCompt as select rID from RoutesNotSchwaste join CompatWaste on RoutesNotSchwaste.wasteType = CompatWaste.wasteType order by rID")
       
        #Step Two : Get avaialble pair of drivers as per given constraints
        (eID1,eID2) = self.get_available_emp_pair(None, dt.datetime(year=date.year, month=date.month,day=date.day,),tid,1)
        if(eID1 is None or eID2 is None):
            driver_found = False
        else:
            driver_found = True
            #ordering the eids for constraints in trip schema
            selected_driver_eID1  = max(eID1,eID2)
            selected_driver_eID2 = min(eID1,eID2)
     
        #Step Three: Scheduling trips, route by route
        trips_schd = 0
        cursor.execute("select * from RoutesNotSchCompt")
        print("selected routes are")
        print("scheduling them now")
        trip_start_time = dt.datetime(date.year, date.month,date.day,8,0) # start at 8:00 am

        for row in cursor:
            selected_facility = self.get_facility(row)
            if(not driver_found or (selected_facility is None) or (trip_start_time.time() < dt.time(8,0,0) or trip_start_time.time() > dt.time(16,0,0)) ):
                continue
            else:
                selected_route = row
                selected_truck = tid
                print(selected_route,selected_truck,trip_start_time,selected_driver_eID1,selected_driver_eID2,selected_facility)
                insert_success = self.insert_Trip(selected_route,selected_truck,trip_start_time,None,selected_driver_eID1,selected_driver_eID2,selected_facility)
                if(insert_success):
                    trips_schd+=1
                    trip_start_time = trip_start_time + self.get_trip_end_time(selected_route,trip_start_time) + dt.timedelta(minutes=31) 
               
        
        return trips_schd
                

    def update_technicians(self, qualifications_file: TextIO) -> int:
        """Given the open file <qualifications_file> that follows the format
        described on the handout, update the database to reflect that the
        recorded technicians can now work on the corresponding given truck type.

        For the purposes of this method, you may assume that no two employees
        in our database have the same name i.e., an employee can be uniquely
        identified using their name.

        Your method should NOT throw an error.
        Instead, only correct entries should be reflected in the database.
        Return the number of successful changes, which is the same as the number
        of valid entries.
        Invalid entries include:
            * Incorrect employee name.
            * Incorrect truck type.
            * The technician is already recorded to work on the corresponding
              truck type.
            * The employee is a driver.

        Hint: We have provided a helper _read_qualifications_file that you
            might find helpful for completing this method.
        """
        try:
            # TODO: implement this method
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return 0

    def partnerlist(self, eid: int) -> list[int]:
        try:
            # TODO: implement this method
            cursor = self.connection.cursor()  
            cursor.execute("select eid1,eid2 from trip where eid1=%s or eid2=%s",[eid,eid])
            partnerlist = []
            for row in cursor:
                if row[0] == eid:
                    if row[1] not in partnerlist:
                        partnerlist.append(row[1])
                elif row[1] == eid:
                    if row[0] not in partnerlist:
                        partnerlist.append(row[0])
            print(partnerlist)
            return partnerlist
            #step 1 : see if u suceesfully getting all teammates
            
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return []

    def workmate_sphere(self, eid: int) -> list[int]:
        """Return the workmate sphere of the driver identified by <eid>, as a
        list of eIDs.

        The workmate sphere of <eid> is:
            * Any employee who has been on a trip with <eid>.
            * Recursively, any employee who has been on a trip with an employee
              in <eid>'s workmate sphere is also in <eid>'s workmate sphere.

        The returned list should NOT include <eid> and should NOT include
        duplicates.

        The order of the returned ids does NOT matter.

        Your method should NOT return an error. If an error occurs, your method
        should simply return an empty list.
        """
        try:
            # TODO: implement this method
            cursor = self.connection.cursor()  
            #partnerlist = self.partnerlist(eid)
            cursor.execute("select * from trip where eid1=%s or eid2=%s",[eid,eid])
            if(cursor.rowcount ==0):
                return []
            #print(partnerlist)
            sphere=[]
            def get_all_eids(eid,sphere):
                if eid not in sphere:
                    sphere.append(eid)
                partnerlist = self.partnerlist(eid)

                for partner in partnerlist:
                    if partner in sphere:
                        continue
                    else:
                        get_all_eids(partner,sphere)

            #do a check to see if eid valid or not

            get_all_eids(eid,sphere) 
            sphere.remove(eid)
            print("All the values in sphere are: ",sphere)
            return sphere
            
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return []

    def schedule_maintenance(self, date: dt.date) -> int:
        """For each truck whose most recent maintenance before <date> happened
        over 90 days before <date>, and for which there is no scheduled
        maintenance up to 10 days following date, schedule maintenance with
        a technician qualified to work on that truck in ascending order of tIDs.

        For example, if <date> is 2023-05-02, then you should consider trucks
        that had maintenance before 2023-02-01, and for which there is no
        scheduled maintenance from 2023-05-02 to 2023-05-12 inclusive.

        Choose the first day after <date> when there is a qualified technician
        available (not scheduled to maintain another truck that day) and the
        truck is not scheduled for a trip or maintenance on that day.

        If there is more than one technician available on a given day, choose
        the one with the lowest eID.

        Return the number of trucks that were successfully scheduled for
        maintenance.

        Your method should NOT throw an error.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.
        """
        try:
            # TODO: implement this method
            #Finding trucks to schedule
            #view clean up            
            cursor = self.connection.cursor()      
            cursor.execute("DROP VIEW IF EXISTS mostRecent CASCADE")
            cursor.execute("DROP VIEW IF EXISTS NinDaysNot CASCADE")
            cursor.execute("DROP VIEW IF EXISTS schAlready CASCADE")
            cursor.execute("DROP VIEW IF EXISTS truckToSch CASCADE")
            
            cursor.execute("create view mostRecent as select tID,max (mDate) as mostRecentDate from Maintenance group by tID")
            cursor.execute("create view NinDaysNot as select tID from mostRecent where mostRecentDate < %s",[date - dt.timedelta(days=90)])
            cursor.execute("create view schAlready as select tID from Maintenance where mDate >= %s and mDate <=%s",[date,date +dt.timedelta(days=10)])
            cursor.execute("create view truckToSch as (select * from NinDaysNot) INTERSECT (select tID from Truck  EXCEPT (select * from schAlready))")
            print("trucks to schd for maint are")
            cursor.execute("select * from truckToSch")
            schd_success = False
            trucks_schd = 0
            for row in cursor:
                print(row)
                selected_truck = row
                #choosing day for maint
                date_to_schd, tech_ID = self.choose_day_tech(row,date)
                print("eID is  and maint date is ",tech_ID,date_to_schd)
                if(tech_ID != -1): # found technicians
                   schd_success = self.insert_Maint(row,tech_ID,date_to_schd)
                   if(schd_success):
                       trucks_schd+=1

            return trucks_schd
           
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            raise ex
            return 0

    def reroute_waste(self, fid: int, date: dt.date) -> int:
        """Reroute the trips to <fid> on day <date> to another facility that
        takes the same type of waste. If there are many such facilities, pick
        the one with the smallest fID (that is not <fid>).

        Return the number of re-routed trips.

        Don't worry about too many trips arriving at the same time to the same
        facility. Each facility has ample receiving facility.

        Your method should NOT return an error. If an error occurs, your method
        should simply return 0 i.e., no trips have been re-routed.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.

        Assume this happens before any of the trips have reached <fid>.
        """
        try:
            # TODO: implement this method
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return 0

    # =========================== Helper methods ============================= #

    @staticmethod
    def _read_qualifications_file(file: TextIO) -> list[list[str, str, str]]:
        """Helper for update_technicians. Accept an open file <file> that
        follows the format described on the A2 handout and return a list
        representing the information in the file, where each item in the list
        includes the following 3 elements in this order:
            * The first name of the technician.
            * The last name of the technician.
            * The truck type that the technician is currently qualified to work
              on.

        Pre-condition:
            <file> follows the format given on the A2 handout.
        """
        result = []
        employee_info = []
        for idx, line in enumerate(file):
            if idx % 2 == 0:
                info = line.strip().split(' ')[-2:]
                fname, lname = info
                employee_info.extend([fname, lname])
            else:
                employee_info.append(line.strip())
                result.append(employee_info)
                employee_info = []

        return result


def setup(dbname: str, username: str, password: str, file_path: str) -> None:
    """Set up the testing environment for the database <dbname> using the
    username <username> and password <password> by importing the schema file
    and the file containing the data at <file_path>.
    """
    connection, cursor, schema_file, data_file = None, None, None, None
    try:
        # Change this to connect to your own database
        connection = pg.connect(
            dbname=dbname, user=username, password=password,
            options="-c search_path=waste_wrangler"
        )
        cursor = connection.cursor()

        schema_file = open("./waste_wrangler_schema.sql", "r")
        cursor.execute(schema_file.read())

        data_file = open(file_path, "r")
        cursor.execute(data_file.read())

        connection.commit()
    except Exception as ex:
        connection.rollback()
        raise Exception(f"Couldn't set up environment for tests: \n{ex}")
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if connection and not connection.closed:
            connection.close()
        if schema_file:
            schema_file.close()
        if data_file:
            data_file.close()


def test_preliminary() -> None:
    """Test preliminary aspects of the A2 methods."""
    ww = WasteWrangler()
    qf = None
    try:
        # TODO: Change the values of the following variables to connect to your
        #  own database:
        dbname = 'csc343h-bhatt122'
        user = 'bhatt122'
        password = 'arpitab@72'

        connected = ww.connect(dbname, user, password)

        # The following is an assert statement. It checks that the value for
        # connected is True. The message after the comma will be printed if
        # that is not the case (connected is False).
        # Use the same notation to thoroughly test the methods we have provided
        assert connected, f"[Connected] Expected True | Got {connected}."

        # TODO: Test one or more methods here, or better yet, make more testing
        #   functions, with each testing a different aspect of the code.

        # The following function will set up the testing environment by loading
        # the sample data we have provided into your database. You can create
        # more sample data files and use the same function to load them into
        # your database.
        # Note: make sure that the schema and data files are in the same
        # directory (folder) as your a2.py file.
        setup(dbname, user, password, './waste_wrangler_data.sql')

        # --------------------- Testing schedule_trip  ------------------------#

        # You will need to check that data in the Trip relation has been
        # changed accordingly. The following row would now be added:
        # (1, 1, '2023-05-04 08:00', null, 2, 1, 1)
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 8, 0))
        assert scheduled_trip, \
            f"[Schedule Trip] Expected True, Got {scheduled_trip}"

        # Can't schedule the same route of the same day.
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 13, 0))
        assert not scheduled_trip, \
            f"[Schedule Trip] Expected False, Got {scheduled_trip}"

        # -------------------- Testing schedule_trips  ------------------------#

        # All routes for truck tid are scheduled on that day
        scheduled_trips = ww.schedule_trips(1, dt.datetime(2023, 5, 3))
        assert scheduled_trips == 0, \
            f"[Schedule Trips] Expected 0, Got {scheduled_trips}"

        # ----------------- Testing update_technicians  -----------------------#

        # This uses the provided file. We recommend you make up your custom
        # file to thoroughly test your implementation.
        # You will need to check that data in the Technician relation has been
        # changed accordingly
        # qf = open('qualifications.txt', 'r')
        # updated_technicians = ww.update_technicians(qf)
        # assert updated_technicians == 2, \
        #     f"[Update Technicians] Expected 2, Got {updated_technicians}"

        # ----------------- Testing workmate_sphere ---------------------------#

        # This employee doesn't exist in our instance
        workmate_sphere = ww.workmate_sphere(2023)
        assert len(workmate_sphere) == 0, \
            f"[Workmate Sphere] Expected [], Got {workmate_sphere}"

        workmate_sphere = ww.workmate_sphere(3)
        # Use set for comparing the results of workmate_sphere since
        # order doesn't matter.
        # Notice that 2 is added to 1's work sphere because of the trip we
        # added earlier.
        assert set(workmate_sphere) == {1, 2}, \
            f"[Workmate Sphere] Expected {{1, 2}}, Got {workmate_sphere}"

        # ----------------- Testing schedule_maintenance ----------------------#

        # You will need to check the data in the Maintenance relation
        scheduled_maintenance = ww.schedule_maintenance(dt.date(2023, 5, 5))
        assert scheduled_maintenance == 7, \
            f"[Schedule Maintenance] Expected 7, Got {scheduled_maintenance}"

        # ------------------ Testing reroute_waste  ---------------------------#

        # There is no trips to facility 1 on that day
        # reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 10))
        # assert reroute_waste == 0, \
        #     f"[Reroute Waste] Expected 0. Got {reroute_waste}"

        # # You will need to check that data in the Trip relation has been
        # # changed accordingly
        # reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 3))
        # assert reroute_waste == 1, \
        #     f"[Reroute Waste] Expected 1. Got {reroute_waste}"
    finally:
        if qf and not qf.closed:
            qf.close()
        ww.disconnect()


if __name__ == '__main__':
    # Un comment-out the next two lines if you would like to run the doctest
    # examples (see ">>>" in the methods connect and disconnect)
    # import doctest
    # doctest.testmod()

    # TODO: Put your testing code here, or call testing functions such as
    #   this one:
    test_preliminary()
