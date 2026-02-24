import json
import mysql.connector
from datetime import datetime

def load_file(file):
    with open(file,"rb") as f:
        data=json.loads(f.read().decode())
        return data

def parser(d):
    flights=[]
    base=d.get("data").get("flightJourneys")
    for i in base:
        if 'flightFare' in i:
            for j in i["flightFare"]:
                detail = {}
                detail["Flight_Key"]=j.get("flightKeys")
                # data.flightJourneys[0].key[0].date
                detail["Refund_Type"]=j.get("refundableType")
                if "fares" in j:
                    for k in j["fares"]:
                        detail["Fare"]=k.get("fareDetails").get("displayFare")
                        if "fareMetadata" in k:
                            for fare in k["fareMetadata"]:
                                detail["Check_in_Baggage"]=fare.get("baggageDetails").get("checkInBaggage")
                                detail["Hand_Baggage"] = fare.get("baggageDetails").get("handBaggage")
                                detail["Cabin_class"]=fare.get("cabinClass")
                if "flightFilter" in j:
                    for filter in j["flightFilter"]:
                        detail["Stops"]=filter.get("stops")

                if "flightDetails" in j:
                    for fli_det in j["flightDetails"]:
                        detail["Origin"]=fli_det.get("origin")
                        detail["Destination"] = fli_det.get("destination")
                        detail["Duration"]=fli_det.get("duration").get("timeInHours")
                        detail["Departure_time"]=fli_det.get("departureTime")
                        detail["Arrival_time"] = fli_det.get("arrivalTime")
                        detail["Airline"]=fli_det.get("headerTextWeb")
                        detail["Airline_code"] = fli_det.get("airlineCode")
                        # if "layover" in fli_det:
                        #     for lay in fli_det.get("layover"):
                        #         if lay is not None:
                        #             detail["Layover_location"]=lay.get("location")
                        #             detail["Layover_duration"] = lay.get("duration")
                        #         else:
                        #             detail["Layover_location"] = "No Layover"
                        #             detail["Layover_duration"] = "No Layover"

                if "isFreeMealAvailable" in j:
                    detail["Meal_Availability"]=j.get("isFreeMealAvailable")
                if "key" in i:
                    for date_ in i["key"]:
                        date=date_.get("date")
                        if date:
                            ddd=datetime.strptime(date, "%d%m%Y").strftime('%Y-%d-%m')
                            detail["Date"]=ddd

                flights.append(detail)
    return flights

def dump_file(data_extracted):
    with open("Extracted_flight.json","wb") as f:
        f.write(json.dumps(data_extracted,indent=4).encode())

file_name="ixigo_flight.json"
json_data=load_file(file_name)
extracted=parser(json_data)
dump_file(extracted)

conn= mysql.connector.connect(
    host="localhost",
    user="root",
    password="actowiz",
    database="flight_db"
)
cursor=conn.cursor()

create_query="""
        CREATE TABLE IF NOT EXISTS flights(
        FlightKey VARCHAR(100) ,
        RefundType VARCHAR(50),
        Date varchar(50),
        Fare int,
        CheckInBaggage VARCHAR(100),
        HandBaggage varchar(50),
        CabinClass varchar(50),
        Stops int,
        Origin varchar(50),
        Destinations varchar(50),
        Duration int,
        DepartureTime varchar(50),
        ArrivalTime varchar(50),
        AirLIne varchar(50),
        AirLineCode varchar(50),
        MealAvailability boolean
        );
"""
cursor.execute(create_query)
for f in extracted:
    query = f"""
    INSERT INTO flights VALUES (
        '{f.get("Flight_Key")}',
        '{f.get("Refund_Type")}',
        '{f.get("Date")}',
        '{f.get("Fare") }',
        '{f.get("Check_in_Baggage")}',
        '{f.get("Hand_Baggage")}',
        '{f.get("Cabin_class")}',
        '{f.get("Stops") }',
        '{f.get("Origin")}',
        '{f.get("Destination")}',
        '{f.get("Duration") }',
        '{f.get("Departure_time")}',
        '{f.get("Arrival_time")}',
        '{f.get("Airline")}',
        '{f.get("Airline_code")}',
        {f.get("Meal_Availability")}
    )
    """
    cursor.execute(query)
conn.commit()