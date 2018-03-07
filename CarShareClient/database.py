#!/usr/bin/env python3

from modules import pg8000
import configparser


# Define some useful variables
ERROR_CODE = 55929

#####################################################
##  Database Connect
#####################################################

def database_connect():
    # Read the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Create a connection to the database
    connection = None
    try:
        connection = pg8000.connect(database=config['DATABASE']['user'],
            user=config['DATABASE']['user'],
            password=config['DATABASE']['password'],
            host=config['DATABASE']['host'])
    except pg8000.OperationalError as e:
        print("""Error, you haven't updated your config.ini or you have a bad
        connection, please try again. (Update your files first, then check
        internet connection)
        """)
        print(e)
    #return the connection to use
    return connection

#####################################################
##  Login
#####################################################

def check_login(email, password):

	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# select details from database where the email and password match given args
		sql = """SELECT nickname,nametitle,namegiven,namefamily,member.address,name,since,subscribed,stat_nrofbookings
				FROM carsharing.member,carsharing.carbay
				WHERE (email=%s OR nickname=%s) AND password=%s AND homebay = carbay.bayid"""
				
		cur.execute(sql,(email, email, password))
		r = cur.fetchone()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error with Database")
		cur.close()
		conn.close()
		return ERROR_CODE

#####################################################
##  Homebay
#####################################################
def update_homebay(email, bayname):

    # ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		sql = """UPDATE member 
			SET homebay=(SELECT bayid FROM carbay WHERE name=%s) 
			WHERE email=%s"""
		cur.execute(sql,(bayname,email))
		conn.commit()
		cur.close()
		conn.close()
		return True;
	except:
        # if there were any errors, rollback and return false
		print("Error with Database")
		conn.rollback()
		cur.close()
		conn.close()
		return False

#####################################################
##  Booking (make, get all, get details)
#####################################################

def make_booking(email, car_rego, date, hour, duration):

	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
	# check if member or car already booked at that time
		sql = """SELECT DISTINCT starttime
				FROM carsharing.member,carsharing.booking
				WHERE ((email=%s OR nickname=%s) OR car=%s) AND madeby=memberno AND (((%s||' '|| cast(to_char(%s::int, '00:')as time without time zone))::timestamp without time zone,
				(%s||' '|| cast(to_char(%s::int, '00:')as time without time zone))::timestamp without time zone+%s*interval '1 hour')
				OVERLAPS(starttime,endtime))
				AND (%s||' '|| cast(to_char(%s::int, '00:')as time without time zone))::timestamp without time zone<current_timestamp(0)::timestamp without time zone"""
			
		cur.execute(sql,(email,email,car_rego,date,hour,date,hour,duration,date,hour))
		r = cur.fetchone()
		if cur.rowcount!=0:
			cur.close()
			conn.close()
			return False
		# if not returned then booking time is good, so try inserting new booking
		sql = """INSERT INTO booking (car,madeby,whenbooked,starttime,endtime) 
				VALUES (%s,(SELECT memberno FROM member WHERE email=%s OR nickname=%s),
				current_timestamp(0)::timestamp without time zone,(%s||' '|| cast(to_char(%s::int, '00:')as time without time zone))::timestamp without time zone,
				(%s||' '|| cast(to_char(%s::int, '00:')as time without time zone))::timestamp without time zone+%s*interval '1 hour')"""
		cur.execute(sql,(car_rego,email,email,date,hour,date,hour,duration))
		# if no error then add one to nrofbookings in member
		sql = """UPDATE member 
				SET stat_nrofbookings = stat_nrofbookings + 1 
				WHERE email=%s OR nickname=%s"""
		cur.execute(sql,(email,email))
		conn.commit()
		cur.close()
		conn.close()
		return True
	except:
		# if there were any errors, return error code
		print("Error communicating with database")
		conn.rollback()
		cur.close()
		conn.close()
		return ERROR_CODE


def get_all_bookings(email):
    
	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# select all bookings associated with given email
		sql = """SELECT car,name,CAST(starttime AS date), CAST(EXTRACT(hour FROM starttime) AS int)
				FROM carsharing.member,carsharing.booking,carsharing.car
				WHERE(email=%s OR nickname=%s) AND memberno=madeby AND car = regno
				ORDER BY booking.starttime DESC"""
				
		cur.execute(sql,(email,email,))
		r = cur.fetchall()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE

def get_booking(b_date, b_hour, car):

	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# get information about specific booking
		sql = """SELECT nickname,car,car.name,CAST(starttime AS date), CAST(EXTRACT(hour FROM starttime) AS int), 
				EXTRACT(EPOCH FROM (endtime-starttime))/3600,CAST(whenbooked AS date),carbay.name
				FROM carsharing.member,carsharing.booking,carsharing.car,carsharing.carbay
				WHERE (%s ||' '|| cast(to_char(%s::int, '00:')as time without time zone))::timestamp without time zone=starttime 
				AND car=%s AND car=regno AND madeby=memberno AND bayid=parkedat"""
				
		cur.execute(sql,(b_date,b_hour,car))
		r = cur.fetchone()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE


#####################################################
##  Car (Details and List)
#####################################################

def get_car_details(regno):
    
	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# get information about a car identified by its regno
		sql = """SELECT regno,car.name,car.make,car.model,year,transmission,category,capacity,carbay.name,carbay.walkscore,carbay.mapurl
				FROM car,carmodel,carbay 
				WHERE regno=%s AND car.make=carmodel.make AND car.model=carmodel.model AND parkedat=bayid"""
				
		cur.execute(sql,[regno])
		r = cur.fetchone()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE

def get_all_cars():
    
	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# select all bookings associated with given email
		sql = """SELECT regno,name,make,model,year,transmission
				FROM car"""
				
		cur.execute(sql,())
		r = cur.fetchall()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE
		
		
#####################################################
##  Bay (detail, list, finding cars inside bay)
#####################################################

def get_all_bays():

    # ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# get all bays
		sql = """SELECT DISTINCT CarBay.name, CarBay.address, COUNT(Car.parkedAt)
				FROM CarBay JOIN Car ON (parkedAt = bayID)
				GROUP BY CarBay.name, address
				ORDER BY CarBay.name"""
				
		cur.execute(sql,())
		r = cur.fetchall()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE

def get_bay(name):
    
	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# get details of specific bay
		sql = """SELECT name,description,address,gps_lat,gps_long
					FROM carbay 
					WHERE name=%s"""
				
		cur.execute(sql,[name])
		r = cur.fetchone()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE
	    
def get_homebay(name):
    
	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
                # get details of specific bay
		sql = """SELECT name
					FROM carbay 
					WHERE name=%s"""
				
		cur.execute(sql,[name])
		r = cur.fetchone()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE
            
def search_bays(search_term):

	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# get bays that match search term
		sql = """SELECT DISTINCT CarBay.name, CarBay.address, COUNT(Car.parkedAt)
				FROM CarBay JOIN Car ON (bayid = parkedAt)
				WHERE LOWER(CarBay.name) ILIKE LOWER(%s)
				OR LOWER(CarBay.address) ILIKE LOWER(%s)
				GROUP BY CarBay.name, address
				ORDER BY CarBay.name"""
		like_pattern = '%{}%'.format(search_term)
		cur.execute(sql,(like_pattern, like_pattern,))
		r = cur.fetchall()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE

def get_cars_in_bay(bay_name):
   
	# ask for the database connection, and get the cursor set up
	conn = database_connect()
	if(conn is None):
		return ERROR_CODE
	cur = conn.cursor()
	try:
		# get information about a car identified by its regno
		sql = """SELECT regno,car.name 
				FROM car,carbay
				WHERE parkedat=bayid AND carbay.name=%s"""
			
		cur.execute(sql,[bay_name,])
		r = cur.fetchall()
		cur.close()
		conn.close()
		return r
	except:
        # if there were any errors, return error code
		print("Error communicating with database")
		cur.close()
		conn.close()
		return ERROR_CODE
            