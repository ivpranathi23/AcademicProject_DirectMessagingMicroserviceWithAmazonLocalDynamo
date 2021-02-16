"""
Project Title: Microblogging
File Name: app.py
Author: Venkata Pranathi Immaneni
Date Created: 1st Oct 2020
Data Modified: 26th Nov 2020
Email: ivpranathi@csu.fullerton.edu

"""

import flask
from flask import request, jsonify, g
import sqlite3
import click
from werkzeug.security import generate_password_hash, check_password_hash
import boto3
from botocore.exceptions import ClientError

app = flask.Flask(__name__)
app.config.from_envvar('APP_CONFIG')

#Get the database connection
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(app.config['DATABASE'])
    return db

#Used to query db and retrieve the contents from the database
def query_db(query, args=(), one=False):
	cur = get_db().execute(query, args)
	retrieve = cur.fetchall()
	cur.close()
	return (retrieve[0] if retrieve else None) if one else retrieve

#Def inserttodb is Used to insert data to database
def inserttodb(query, args=()):
	dbConn = get_db()
	cur = dbConn.cursor()
	cur.execute(query, args)
	dbConn.commit()


#Def jsonResponse returns the parameters in JSON format.
def jsonResponse(statusCode, message):

	return flask.jsonify(ContentLanguage="en-US", ContentType = "application/json", StatusCode=statusCode, Message=message)


#Define createUser is used to sign up for a new user.
#It check whether the user already exists, else creates the user and adds the entry to the database.
#This api has POST method. It takes username, password, and email in JSON format.

@app.route('/v1/createUser', methods=['POST'])
def createUser():

	#Checks whether the content type of the request params is in JSON format, else return an error message.
	if request.headers['Content-Type'] != 'application/json':
		return jsonResponse(400,"Bad Request. Content type should be json")

	#Retrieves username, password and email from the POST request
	username = request.json.get('username')
	password = request.json.get('password')
	email = request.json.get('email')

	#Checks whether all the required parameters are passed else returns the error message.
	if username is None or password is None or email is None:
		return jsonResponse(400,"Missing Username or Password or email fields")

	#Checks whether the user already exists. If exists, it returns the error message that 'User already exists'.
	findUser = query_db('Select * from users where username = ?',
			 [username], one = True)
	if findUser is not None:
		return jsonResponse(409,"User Already Exists")

	#Hashing the password to store in database
	hashedPassword = generate_password_hash(password)

	#Inserting the new user details to the database.
	query = "INSERT INTO users(username, email, password) VALUES (?,?,?)"
	inserttodb(query, [username, email, hashedPassword])
	return jsonResponse(200,"User Created Successfully")


#Define getUsers is used to retrieves the list of existing users.

@app.route('/v1/getUsers', methods=['GET'])
def getUsers():
	retrieveAllUsers = query_db('Select * from users')
	return flask.jsonify(retrieveAllUsers)


#Define authenticateUser is used authenticate user during sig-in to check whether user account already exists.
#If the user does not exists, it returns the error message, that user does not exist.
#This api has POST method. It takes username, password in JSON Format and verfies in the database.

@app.route('/v1/authenticateUser', methods=['POST'])
def authenticateUser():

	#Checks whether the content type of the request params is in JSON format, else return an error message.
	if request.headers['Content-Type'] != 'application/json':
		return jsonResponse(400, "Bad Request. Content type should be json")

	#Retrieves username and password from the POST request
	username = request.json.get('username')
	password = request.json.get('password')

	#Checks whether all the required parameters are passed else returns the error message.
	if username is None or password is None:
		return jsonResponse(401, "Authentication failed!!Missing Username or Password fields")

	#Retrieves the user password from the database to verfiy it against the hashed password
	userPassword = query_db('Select password from users where username = ?', [username], one=True)

	#Checks whether the user exists. If the user password is nil, it means user does not exists, with the given username. Returns error message if user does not exists.
	if userPassword is None:
		return jsonResponse(401, "Authentication failed!! User not found")
	else:

		#checks the user password against the hashed password, that is retrieved from the database
		result = check_password_hash(userPassword[0], password)
		if result == True:
			return jsonResponse(200, result)
		else:
			return flask.jsonify(ContentLanguage="en-US", ContentType = "application/json", StatusCode=401, Message=result, Error="Authentication Failed")

#Define addFollower is used follow a particular user.
#This api has POST method. It takes username, usernameToFollow in JSON Format

@app.route('/v1/addFollower', methods=['POST'])
def addFollower():

	#Checks whether the content type of the request params is in JSON format, else return an error message.
	if request.headers['Content-Type'] != 'application/json':
		return jsonResponse(400, "Bad Request. Content type should be json")

	#Retrieves username and usernameToFollow from the POST request
	username = request.json.get('username')
	usernameToFollow = request.json.get('usernameToFollow')

	#Checks whether username and usernameToFollow are passed in POST method, else returns the error message.
	if username is None or usernameToFollow is None:
		return jsonResponse(400, "Bad Request. Missing Username or UsernameToFollow")

	#If the username wants to follow himself, this is not allowed. It returns an error message if username and username to follow are same.
	if username == usernameToFollow:
		return jsonResponse(400, "Bad Request. User cannot follow himserlf/herself")

	#Check if the user is already following the user
	checkduplicate = query_db('Select username from followers where username = ? AND followerUsername = ?', [username, usernameToFollow], one=True)

	if checkduplicate:
		return jsonResponse(409, "You are already following this user")

	#Check whether user exists - that is user created his/her account, else return an error message that user not found
	getUsername = query_db('Select username from users where username = ?', [username], one=True)
	if getUsername is None:
		return jsonResponse(404, "User not found")

	#Check whether usernameToFollow exists - that is user created his/her account, else return an error message that user not found
	getUsernameToFollow = query_db('Select username from users where username = ?', [usernameToFollow], one=True)
	if getUsernameToFollow is None:
		return jsonResponse(404, "User to Follow not found")

	#Insert username and usernameToFollow into followers table of database.
	query = "INSERT INTO followers(username, followerUsername) VALUES (?,?)"
	inserttodb(query, [username, usernameToFollow])

	#Returns success message once the user is inserted to database
	return jsonResponse(200, "Follower Added Successfully")




#Define removeFollower is used unfollow a particular user.
#This api has POST method. It takes username, usernameToRemove in JSON Format
@app.route('/v1users/removeFollower', methods=['DELETE'])
def removeFollower():

	#Checks whether the content type of the request params is in JSON format, else return an error message.
	if request.headers['Content-Type'] != 'application/json':
		return jsonResponse(400, "Bad Request. Content type should be json")

	#Retrieves username and usernameToRemove from the POST request
	username = request.json.get('username')
	usernameToRemove = request.json.get('usernameToRemove')

	#Checks whether username and usernameToRemove are passed in POST method, else returns the error message.
	if username is None or usernameToRemove is None:
		return jsonResponse(400, "Missing Username or usernameToRemove fields")

	#If the username wants to unfollow himself, this is not allowed. It returns an error message if username and usernameToRemove are same
	if username == usernameToRemove:
		return jsonResponse(400, "Bad Request. User cannot follow himserlf/herself")

	#Check whether user exists - that is user created his/her account, else return an error message that user not found
	getUsername = query_db('Select username from users where username = ?', [username], one=True)
	if getUsername is None:
		return jsonResponse(404, "User not found")

	#Check whether usernamtToRemove exists in database - that is user created his/her account, else return an error message that user not found
	getUsernameToRemove = query_db('Select username from users where username = ?', [usernameToRemove], one=True)
	if getUsernameToRemove is None:
		return jsonResponse(404, "User to remove not found")

	#Delete the row containing username and usernameToRemove from followers table of users database
	queryToDelete = "DELETE FROM FOLLOWERS WHERE username=? AND followerUsername=? "
	inserttodb(queryToDelete, [username, usernameToRemove])

	return jsonResponse(200, "Follower Removed Successfully")
