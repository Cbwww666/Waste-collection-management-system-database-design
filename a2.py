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
            expect_start = time - dt.timedelta(minutes=30)
            expect_end = time + dt.timedelta(minutes=30)
            expect_day = time.date()

            #     Get wastetype from rid
            connection = self.connection
            cursor = connection.cursor()
            cursor.execute("select * from waste_wrangler.route where rid = " + str(rid))
            data = cursor.fetchone()
            if len(data) == 0:
                return False
            wastetype = data[1]

            #     Get trip by rid and time period
            connection = self.connection
            expect_day_after = time + dt.timedelta(days=1)
            expect_day_before = time - dt.timedelta(days=1)

            cursor = connection.cursor()
            sql = "select rid from waste_wrangler.trip where rid = '" + str(rid) + "' and ttime < '" + str(expect_day_after) + "' and ttime > '" + str(time) + "'"
            cursor.execute(sql)
            data = cursor.fetchall()
            if len(data) > 0:
                return False

            #     Get fid by wastetype
            connection = self.connection
            expect_day_after = time + dt.timedelta(days=1)
            expect_day_before = time - dt.timedelta(days=1)

            cursor = connection.cursor()
            sql = "select fid from waste_wrangler.facility where wastetype = '" + str(wastetype) + "'"
            cursor.execute(sql)
            data = cursor.fetchone()
            fid = data[0]

            #     Get trucktypes from wastetype
            connection = self.connection
            cursor = connection.cursor()
            sql = "select * from waste_wrangler.trucktype where wastetype = '" + str(wastetype) + "'"
            cursor.execute(sql)
            data = cursor.fetchall()
            trucktypes = []
            for d in data:
                trucktypes.append(d[0])

            #     Get truck id from trucktype
            connection = self.connection
            cursor = connection.cursor()
            sql = "select tid from waste_wrangler.truck where trucktype in ('"
            for d in trucktypes:
                sql = sql + d + "','"
            sql = sql[:len(sql)-2]
            sql = sql + ") order by capacity desc"

            cursor.execute(sql)
            data = cursor.fetchall()
            tid = []
            for d in data:
                tid.append(d[0])

            #     Get ttid from tid and time period
            ttid = 0
            for d in tid:
                connection = self.connection
                cursor = connection.cursor()
                sql = "select tid from waste_wrangler.maintenance where tid = '" + str(d) + "' and mdate < '" + str(time) + "'"

                cursor.execute(sql)
                data = cursor.fetchall()
                if len(data) == 0:
                    continue
                connection = self.connection
                cursor = connection.cursor()
                sql = "select tid from waste_wrangler.trip where ttime > '" + str(expect_start) + "' and ttime <'" + str(expect_end) + "'"

                cursor.execute(sql)
                data = cursor.fetchall()
                if len(data) > 0 :
                    continue
                ttid = d
                break

            if ttid == 0:
                return False

            #     Get eid from trucktype  for  driver
            connection = self.connection
            cursor = connection.cursor()
            sql = "select eid from waste_wrangler.driver where trucktype in ('"
            for d in trucktypes:
                sql = sql + d + "','"
            sql = sql[:len(sql)-2]
            sql = sql + ")"

            cursor.execute(sql)
            data = cursor.fetchall()
            eid = []
            for d in data:
                eid.append(d[0])

            #     Get normal employment from trucktrype
            connection = self.connection
            cursor = connection.cursor()
            sql = "select eid from waste_wrangler.employee where eid in ('"
            for d in eid:
                sql = sql + str(d) + "','"
            sql = sql[:len(sql)-2]
            sql = sql + ") order by hiredate, eid"

            cursor.execute(sql)
            data = cursor.fetchall()
            eid = []
            for d in data:
                eid.append(d[0])

            #     Fo the filter about driver
            eiddriver = 0
            for d in eid:
                connection = self.connection
                cursor = connection.cursor()
                sql = "select tid from waste_wrangler.maintenance where tid = '" + str(d) + "' and mdate < '" + str(expect_day) + "'"

                cursor.execute(sql)
                data = cursor.fetchall()
                if len(data) == 0:
                    continue
                connection = self.connection
                cursor = connection.cursor()
                sql = "select tid from waste_wrangler.trip where ttime > '" + str(expect_start) + "' and ttime <'" + str(expect_end) + "'"

                cursor.execute(sql)
                data = cursor.fetchall()
                if len(data) > 0 :
                    continue
                eiddriver = d
                break
            if eiddriver == 0:
                return False

            connection = self.connection
            cursor = connection.cursor()
            sql = "select eid from waste_wrangler.employee where eid != " + str(eiddriver) + " order by hiredate"

            cursor.execute(sql)
            data = cursor.fetchall()
            eid = []

            for d in data:
                eid.append(d[0])
            #     Do the normal employment filter

            eidpartner = 0
            for d in eid:
                connection = self.connection
                cursor = connection.cursor()
                sql = "select tid from waste_wrangler.maintenance where tid = '" + str(d) + "' and mdate < '" + str(time) + "'"

                cursor.execute(sql)
                data = cursor.fetchall()
                if len(data) == 0:
                    continue
                connection = self.connection
                cursor = connection.cursor()
                sql = "select tid from waste_wrangler.trip where ttime > '" + str(expect_start) + "' and ttime <'" + str(expect_end) + "'"

                cursor.execute(sql)
                data = cursor.fetchall()
                if len(data) > 0 :
                    continue
                eidpartner = d
                break
            if eidpartner == 0:
                return False
            ans = [ttid, eiddriver, eidpartner]

            #     Insert into trip
            connection = self.connection
            cursor = connection.cursor()
            today = time.date()
            tomorrow = today + dt.timedelta(days=1)
            sql = "select tid from waste_wrangler.trip where ttime > '" + str(today) + "' and ttime <'" + str(tomorrow) + "' and rid = '" + str(rid) + "'"
            cursor.execute(sql)
            data = cursor.fetchall()
            if data is None or len(data) == 0:
                connection = self.connection
                cursor = connection.cursor()
                sql = "INSERT INTO waste_wrangler.trip(rid, tid, ttime, eid1, eid2, fid) VALUES (" + str(rid) + ", '" + str(ttid) + "' , '" + time.strftime("%Y-%m-%d %H:%M:%S") + "' , '" + str(eiddriver) + "' , '" + str(eidpartner) + "', '" + str(fid) + "')"
                cursor.execute(sql)
                connection.commit()
            else:
                return False
            return True
        except pg.Error as ex:
                # You may find it helpful to uncomment this line while debugging,
                # as it will show you all the details of the error that occurred:
                # raise ex
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
        connection = self.connection
        expect_day_after = date + dt.timedelta(days=1)
        expect_day_before = date - dt.timedelta(days=1)
        #    Find Tid
        cursor = connection.cursor()
        sql = "select tid from waste_wrangler.trip where tid = '" + str(tid) + "' and ttime < '" + str(expect_day_after) + "' and ttime > '" + str(date) + "'"
        cursor.execute(sql)
        data = cursor.fetchall()
        if len(data) > 0:
            return 0
        usedTid = []
        for d in data:
            usedTid.append(d[0])

        #  Find trucktype by tid
        connection = self.connection
        cursor = connection.cursor()
        sql = "select distinct(trucktype) from waste_wrangler.truck where tid = '" + str(tid) + "')" # TA: added =

        cursor.execute(sql)
        data = cursor.fetchall()
        trucktype = data[0]

        #  Find wasteType by trucktype
        connection = self.connection

        cursor = connection.cursor()
        sql = "select wastetype from waste_wrangler.trucktype where trucktype = '" + str(trucktype) + "'"
        cursor.execute(sql)
        data = cursor.fetchall()
        wastetypes = []
        for d in data:
            if d[0] not in wastetypes:
                wastetypes.append(data[0])

        #     Find Rid by wasteType

        connection = self.connection
        cursor = connection.cursor()
        sql = "select rid from waste_wrangler.route where wastetype in ('"
        for d in wastetypes:
            sql = sql + d + "','"
        sql = sql[:len(sql)-2]
        sql = sql + ")"
        cursor.execute(sql)
        data = cursor.fetchone()
        if len(data) == 0:
            return False
        rid = data[0]

        #     Find eid by truckType
        connection = self.connection
        cursor = connection.cursor()
        sql = "select distinct(eid) from waste_wrangler.driver where trucktype in ('"
        for d in trucktype:
            sql = sql + d + "','"
        sql = sql[:len(sql)-2]
        sql = sql + ")"

        cursor.execute(sql)
        data = cursor.fetchall()
        eid = []
        for d in data:
            eid.append(d[0])

        #  Find eid who is occupied on the date
        connection = self.connection
        expect_day_after = date + dt.timedelta(days=1)
        expect_day_before = date - dt.timedelta(days=1)

        cursor = connection.cursor()
        sql = "select rid from waste_wrangler.trip where eid1 in ('"
        for d in eid:
            sql = sql + str(d) + "','"
        sql = sql[:len(sql)-2]
        sql = sql + ") or eid2 in ('"
        for d in eid:
            sql = sql + str(d) + "','"
        sql = sql[:len(sql)-2]
        sql = sql + ") and ttime < '" + str(expect_day_after) + "' and ttime > '" + str(expect_day_before) + "'"

        cursor.execute(sql)
        data = cursor.fetchall()
        ocEid = []
        for d in data :
            ocEid.append(d[0])

         #  Find available drivers
        filtedDriver = []
        for d in eid:
            if d in ocEid:
                continue
            filtedDriver.append(d)

        connection = self.connection
        cursor = connection.cursor()
        sql = "select eid from waste_wrangler.employee where eid not in ('"
        for d in filtedDriver:
            sql = sql + str(d) + "','"
        sql = sql[:len(sql)-2]
        sql = sql + ")"

        cursor.execute(sql)
        data = cursor.fetchall()
        eid = []
        for d in data:
            eid.append(d[0])

        #     Find occupied employees
        connection = self.connection
        expect_day_after = date + dt.timedelta(days=1)
        expect_day_before = date - dt.timedelta(days=1)

        cursor = connection.cursor()
        sql = "select rid from waste_wrangler.trip where eid1 in ('"
        for d in eid:
            sql = sql + str(d) + "','"
        sql = sql[:len(sql)-2]
        sql = sql + ") or eid2 in ('"
        for d in eid:
            sql = sql + str(d) + "','"
        sql = sql[:len(sql)-2]
        sql = sql + ") and ttime < '" + str(expect_day_after) + "' and ttime > '" + str(expect_day_before) + "'"

        cursor.execute(sql)
        data = cursor.fetchall()
        ocEid = []
        for d in data:
            ocEid.append(d[0])

        #     Find available employees
        filtedEid = []
        for d in eid:
            if d in ocEid:
                continue
            filtedEid.append(d)

        #     Find FID
        connection = self.connection
        expect_day_after = date + dt.timedelta(days=1)
        expect_day_before = date - dt.timedelta(days=1)

        cursor = connection.cursor()
        sql = "select fid from waste_wrangler.facility where wastetype ('"
        for d in wastetypes:
            sql = sql + d + "','"
        sql = sql[:len(sql)-2]
        sql = sql + ")"
        data = cursor.fetchone()
        fid = data[0]

        #     Insert Trip
        ans = 0
        num = 0
        now = date
        offwork =  date.date() + dt.timedelta(hours=16)
        if len(filtedDriver) <= len(filtedEid):
            num = len(filtedDriver)
            for i in range(0, num):
                connection = self.connection
                cursor = connection.cursor()
                sql = "INSERT INTO waste_wrangler.trip(rid, tid, ttime, eid1, eid2, fid) VALUES (" + str(rid) + ", '" + str(tid) + "' , '" + now.strftime("%Y-%m-%d %H:%M:%S") + "' , '" + str(filtedDriver[i]) + "' , '" + str(filtedEid[i]) + "', '" + str(fid) + "')"
                try:
                    cursor.execute(sql)
                    connection.commit()
                except pg.Error as ex:
                    continue
                ans += 1
                now = now + dt.timedelta(minutes=30)
                if now > offwork:
                    return ans
        else:
            num = len(filtedEid) + (len(filtedDriver) - len(filtedEid)) // 2
            for i in range(0, len(filtedEid)):
                connection = self.connection
                cursor = connection.cursor()
                sql = "INSERT INTO waste_wrangler.trip(rid, tid, ttime, eid1, eid2, fid) VALUES (" + str(rid) + ", '" + str(tid) + "' , '" + now.strftime("%Y-%m-%d %H:%M:%S") + "' , '" + str(filtedDriver[i]) + "' , '" + str(filtedEid[i]) + "', '" + str(fid) + "')"
                try:
                    cursor.execute(sql)
                    connection.commit()
                except pg.Error as ex:
                    continue
                ans += 1
                now = now + dt.timedelta(minutes=30)
                if now > offwork:
                    return ans
            for i in range (len(filtedEid), len(filtedDriver)):
                connection = self.connection
                cursor = connection.cursor()
                sql = "INSERT INTO waste_wrangler.trip(rid, tid, ttime, eid1, eid2, fid) VALUES (" + str(rid) + ", '" + str(tid) + "' , '" + now.strftime("%Y-%m-%d %H:%M:%S") + "' , '" + str(filtedDriver[i]) + "' , '" + str(filtedDriver[i]) + "', '" + str(fid) + "')"
                try:
                    cursor.execute(sql)
                    connection.commit()
                except pg.Error as ex:
                    continue
                ans += 1
                now = now + dt.timedelta(minutes=30)
                if now > offwork:
                    return ans

        return num

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
            file = qualifications_file
            ans = 0
            n = 0
            eid = 0
            while (line := file.readline().rstrip()):
                if n % 2 == 0:
                    n += 1
                    name = line
                    d = name.split(" ")
                    #             Find eid
                    connection = self.connection
                    cursor = connection.cursor()
                    selectedName = d[len(d)-2] + " " + d[len(d)-1]
                    sql = "select eid from waste_wrangler.employee where name = '" + str(selectedName) + "'"

                    cursor.execute(sql)
                    data = cursor.fetchone()
                    if len(data) == 0:
                        continue
                    eid = data[0]
                    #             Filter Driver
                    connection = self.connection
                    cursor = connection.cursor()
                    selectedName = d[len(d)-2] + " " + d[len(d)-1]
                    sql = "select eid from waste_wrangler.driver where eid = '" + str(eid) + "'"

                    cursor.execute(sql)
                    data = cursor.fetchone()

                    if data is not None :
                        eid = 0
                else:
                    n+=1
                    if eid == 0:
                        continue

                    connection = self.connection

                    cursor = connection.cursor()
                    sql = "select * from waste_wrangler.trucktype where trucktype = '" + str(line) + "'"
                    cursor.execute(sql)
                    data = cursor.fetchall()
                    if len(data) == 0:
                        continue
                    #   Filter current pairs
                    connection = self.connection
                    cursor = connection.cursor()
                    sql = "select * from waste_wrangler.technician where eid = '" + str(eid) + "' and trucktype = '" + str(line) + "'"
                    cursor.execute(sql)
                    data = cursor.fetchall()
                    if len(data) != 0:
                        continue
                    #             Insert Data
                    connection = self.connection
                    cursor = connection.cursor()
                    sql = "INSERT INTO waste_wrangler.technician(eid, trucktype) VALUES (" + str(eid) + ", '" + str(line) + "')"
                    cursor.execute(sql)
                    connection.commit()

                    eid = 0
                    ans += 1
            return ans
        except pg.Error as ex:
                # You may find it helpful to uncomment this line while debugging,
                # as it will show you all the details of the error that occurred:
                # raise ex
                return 0

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
            eid_list = []
            eid_list.append(eid)
            ans = [eid]
            while True:
             # Find the current sphere
                connection = self.connection

                cursor = connection.cursor()
                sql = "select eid1, eid2 from waste_wrangler.trip where eid1 in ('"
                for d in eid_list:
                    sql = sql + str(d) + "','"
                sql = sql[:len(sql)-2]
                sql = sql + ") or eid2 in ('"
                for d in eid_list:
                    sql = sql + str(d) + "','"
                sql = sql[:len(sql)-2]
                sql = sql + ")"

                cursor.execute(sql)
                data = cursor.fetchall()

                cur = []
                for d in data:
                    if d[0] not in eid_list and d[0] not in ans:
                        ans.append(d[0])
                        cur.append(d[0])
                    if d[1] not in eid_list and d[1] not in ans:
                        ans.append(d[1])
                        cur.append(d[1])
                eid_list = cur
                if len(eid_list) == 0:
                    return ans[1:]
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
            expect_day_before = date - dt.timedelta(days=90)
            expect_day_after_start = date + dt.timedelta(days=1)
            expect_day_after_to = date + dt.timedelta(days=10)

            #     Find the tid from date period
            connection = self.connection
            cursor = connection.cursor()
            sql = "select tid from waste_wrangler.maintenance where mdate < '" + str(expect_day_before) + "'"

            cursor.execute(sql)
            data = cursor.fetchall()
            tid = []
            for d in data:
                if d[0] not in tid:
                    tid.append(d[0])

            #     Find the filter of tid
            connection = self.connection
            cursor = connection.cursor()
            sql = "select tid from waste_wrangler.maintenance where mdate > '" + str(expect_day_after_start) + "' and mdate < '" + str(expect_day_after_to) + "'"

            cursor.execute(sql)
            data = cursor.fetchall()
            noTid = []
            for d in data:
                if d not in noTid:
                    noTid.append(d)

            #     Find the selective tids
            if len(noTid) != 0:
                connection = self.connection
                cursor = connection.cursor()
                sql = "select tid from waste_wrangler.truck where tid Not in ('"
                for d in noTid:
                    sql = sql + str(d) + "','"
                sql = sql[:len(sql)-2]
                sql = sql + ")"

                cursor.execute(sql)
                data = cursor.fetchall()
                for d in data:
                    if d not in tid:
                        tid.append(d)

            else:
                connection = self.connection
                cursor = connection.cursor()
                sql = "select tid from waste_wrangler.truck "

                cursor.execute(sql)
                data = cursor.fetchall()
                for d in data:
                    if d[0] not in tid:
                        tid.append(d[0])

            tid.sort()

            for d in tid:
                currentid=d
                #         Find trucktype from tid
                connection = self.connection
                cursor = connection.cursor()
                sql = "select distinct(trucktype) from waste_wrangler.truck where tid = '" + str(d) + "'"

                cursor.execute(sql)
                data = cursor.fetchall()
                trucktypes = [data[0][0]]

                #         Find eid from trucktype

                connection = self.connection
                cursor = connection.cursor()
                sql = "select distinct(eid) from waste_wrangler.technician where trucktype in ('"
                for d in trucktypes:
                    sql = sql + str(d) + "','"
                sql = sql[:len(sql)-2]
                sql = sql + ")"

                cursor.execute(sql)
                data = cursor.fetchall()
                eid = []
                for d in data:
                    if d[0] not in eid:
                        eid.append(d[0])
                eid.sort()

                chooseEid = ""
                currentTime = date

                while (True):
            #             Check which date is the earliest date
                    currentTime = currentTime + dt.timedelta(days=1)
                    n = 0
                    # for each eid, choose whether they are used or not and choose the earliest date to use. then insert into database
                    for chooseEid in eid:
                        connection = self.connection
                        cursor = connection.cursor()
                        sql = "select eid from waste_wrangler.maintenance where mdate = '" + currentTime.strftime("%Y-%m-%d") + "' and eid = '" + str(chooseEid) + "'"

                        cursor.execute(sql)
                        data = cursor.fetchall()
                        if len(data) == 0:
                            connection = self.connection
                            cursor = connection.cursor()
                            sql = "INSERT INTO waste_wrangler.maintenance(tid, eid, mdate) VALUES (" + str(currentid) + ", '" + str(chooseEid) + "', '" + currentTime.strftime("%Y-%m-%d") + "')"
                            cursor.execute(sql)
                            connection.commit()
                            n = 1
                            break
                    if n == 0:
                        continue
                    else:
                        break

            return len(tid)
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
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
            connection = self.connection
            cursor = connection.cursor()
            sql = "select wastetype from waste_wrangler.facility where fid = '" + str(fid) + "'"

            cursor.execute(sql)
            data = cursor.fetchall()
            wasteType = data[0][0]

            cur = date + dt.timedelta(days=1)
            #     Find fid from wastetype
            connection = self.connection
            cursor = connection.cursor()
            sql = "select fid from waste_wrangler.facility where wastetype = '" + str(wasteType) + "'"

            cursor.execute(sql)
            data = cursor.fetchall()
            fids = []
            for d in data:
                if d[0] not in fids and d[0] != fid:
                    fids.append(d[0])
            if len(fids) == 0:
                return 0

            # Find fid

            connection = self.connection
            expect_day_after = date + dt.timedelta(days=1)
            expect_day_before = date - dt.timedelta(days=1)
            cursor = connection.cursor()
            sql = "select * from waste_wrangler.trip where fid = '" + str(fid) + "' and ttime > '" + str(date) + "' and ttime < '" + str(expect_day_after) + "'"
            cursor.execute(sql)
            data = cursor.fetchall()

            if len(data) != 0:
                # Update data
                sql = "UPDATE waste_wrangler.trip set fid = " + str(fids[0]) + " where rid = " + str(data[0][1]) + " and ttime = '" + data[0][2].strftime("%Y-%m-%d %H:%M:%S") + "'"
                connection = self.connection
                cursor = connection.cursor()
                cursor.execute(sql)
                connection.commit()

            return len(data)
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
        dbname = 'postgres'
        user = ''
        password = ''

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
        qf = open('qualifications.txt', 'r')
        updated_technicians = ww.update_technicians(qf)
        assert updated_technicians == 2, \
            f"[Update Technicians] Expected 2, Got {updated_technicians}"

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
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 10))
        assert reroute_waste == 0, \
            f"[Reroute Waste] Expected 0. Got {reroute_waste}"

        # You will need to check that data in the Trip relation has been
        # changed accordingly
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 3))
        assert reroute_waste == 1, \
            f"[Reroute Waste] Expected 1. Got {reroute_waste}"
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
