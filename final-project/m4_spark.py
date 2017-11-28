import psycopg2
import sys, os, configparser, csv
from pyspark import SparkConf, SparkContext

log_path = "/home/hadoop/logs/" # don't change this
aws_region = "us-east-1"  # don't change this
s3_bucket = "cs327e-fall2017-final-project" # don't change this
persons_file = "s3a://" + s3_bucket + "/cinemalytics/persons.csv" # dataset for milestone 4
singer_songs_file = "s3a://" + s3_bucket + "/cinemalytics/singer_songs.csv" # dataset for milestone 4
songs_file = "s3a://" + s3_bucket + "/cinemalytics/songs.csv" # dataset for milestone 4
title_songs_file = "s3a://" + s3_bucket + "/cinemalytics/title_songs.csv" # dataset for milestone 4
titles_file = "s3a://" + s3_bucket + "/cinemalytics/titles.csv" # dataset for milestone 4

# global variable sc = Spark Context
sc = SparkContext()

# global variables for RDS connection
rds_config = configparser.ConfigParser()
rds_config.read(os.path.expanduser("~/config"))
rds_database = rds_config.get("default", "database") 
rds_user = rds_config.get("default", "user")
rds_password = rds_config.get("default", "password")
rds_host = rds_config.get("default", "host")
rds_port = rds_config.get("default", "port")

def init():
    # set AWS access key and secret account key
    cred_config = configparser.ConfigParser()
    cred_config.read(os.path.expanduser("~/.aws/credentials"))
    access_id = cred_config.get("default", "aws_access_key_id") 
    access_key = cred_config.get("default", "aws_secret_access_key") 
    
    # spark and hadoop configuration
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

################## general utility function ##################################

def print_rdd(rdd, logfile):
  f = open(log_path + logfile, "w") 
  results = rdd.collect() 
  counter = 0
  for result in results:
    counter = counter + 1
    f.write(str(result) + "\n")
    if counter > 30:
      break
  f.close()

################## initialize ##################################
init()

################## take care of the Songs table ##################################
def parse_line_Songs(line):
  arr = line.strip().split(",")
  song_id = arr[0]
  song_title = ",".join(arr[1:-1])
  song_duration = arr[-1]
  return tuple([song_id, song_title, song_duration])

def save_Songs_to_db(list_of_tuples):
  conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
  conn.autocommit = True
  cur = conn.cursor()

  #insert every record into 
  for t in list_of_tuples:
    try:
      song_id, song_title, song_duration = t
      insert_stmt = "insert into Songs values (%s, %s, %s)"


      # Add logic to perform insert statement (step 7)
      cur.execute(insert_stmt, (song_id, song_title, song_duration))

    except Exception as e:
      print "Error in save_Songs_to_db: ", e.message
   
  cur.close()
  conn.close()

songs_rdd = sc.textFile(songs_file).map(parse_line_Songs).distinct()
songs_rdd.foreachPartition(save_Songs_to_db)


################## take care of the Singer_Songs table ##################################
def parse_line_Singer_Songs(line):
  return tuple(line.strip().split(","))

def parse_line_Persons(line):
  arr = line.strip().split(",")
  person_id = arr[0]
  primary_name = arr[1]
  gender = arr[2]
  dob = None
  if arr[3].count("-") == 2:
    dob = arr[3].split("-")[0]

  return (person_id, (primary_name, dob, gender))

def save_Singer_Songs_to_db(list_of_tuples):
  conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
  conn.autocommit = True
  cur = conn.cursor()

  #insert every record into 
  for t in list_of_tuples:
    person_id, song_id = t
    insert_stmt = "insert into Singer_Songs values (%s, %s)"

    try:
      # Add logic to perform insert statement (step 7)
      cur.execute(insert_stmt, (person_id, song_id))

    except Exception as e:
      print "Error in save_Singer_Songs_to_db: ", e.message, t
   
  cur.close()
  conn.close()

def singer_songs_helper(t):
  k, v = t
  person_id, song_id = k, v[0]
  return (person_id, song_id)

persons_rdd = sc.textFile(persons_file).map(parse_line_Persons)
singer_songs_rdd = sc.textFile(singer_songs_file).map(parse_line_Singer_Songs).distinct()
# refine singer_songs
singer_songs_rdd = singer_songs_rdd.join(persons_rdd).map(singer_songs_helper)
singer_songs_rdd.foreachPartition(save_Singer_Songs_to_db)



################## take care of the Person_Basics table ##################################
def drop_song(t):
  k, v = t
  person_id = k
  primary_name, dob, gender = v[1]
  return (person_id, (primary_name, dob, gender))

persons_rdd = sc.textFile(persons_file).map(parse_line_Persons)
rdd_joined = singer_songs_rdd.join(persons_rdd)
rdd_joined = rdd_joined.map(drop_song).distinct()

def save_Persons_to_db(list_of_tuples):
  conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
  conn.autocommit = True
  cur = conn.cursor()

  for t in list_of_tuples:
    person_id, v = t
    primary_name, dob, gender = v

    insert_stmt = "insert into Person_Basics values (%s, %s, %s, %s, %s)"

    try:
      # Add logic to perform insert statement (step 7)
      cur.execute(insert_stmt, (person_id, primary_name, dob, None, gender))

    except Exception as e:
      print "Error in save_Persons_to_db: ", e.message
   
  cur.close()
  conn.close()
rdd_joined.foreachPartition(save_Persons_to_db)


# ################## take care of the Title_Songs table ##################################
def parse_line_Title_Songs(line):
  arr = line.split(",")
  song_id = arr[0]
  movie_id = arr[1]
  return (movie_id, song_id)

def parse_line_Titles(line):
  arr = line.split(",")
  movie_id = arr[0]
  imdb_id = arr[1]
  return (movie_id, imdb_id)

def title_songs_helper(t):
  v = t[1]
  song_id, title_id = v
  return (title_id, song_id)


title_songs_rdd = sc.textFile(title_songs_file).map(parse_line_Title_Songs)
titles_rdd = sc.textFile(titles_file).map(parse_line_Titles)

rdd_joined = title_songs_rdd.join(titles_rdd)
rdd_joined = rdd_joined.map(title_songs_helper).distinct()


def save_Title_Songs_to_db(list_of_tuples):
  conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
  conn.autocommit = True
  cur = conn.cursor()

  for t in list_of_tuples:
    title_id, song_id = t

    insert_stmt = "insert into Title_Songs_Temp values (%s, %s)"
    try:
      cur.execute(insert_stmt, (title_id, song_id))

    except Exception as e:
      print "Error in save_Title_Songs_to_db: ", e.message

  cur.close()
  conn.close()


conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
conn.autocommit = True
cur = conn.cursor()
stmt = "create table if not exists Title_Songs_Temp (title_id varchar(10), song_id varchar(10))"
cur.execute(stmt)
cur.close()
conn.close()

rdd_joined.foreachPartition(save_Title_Songs_to_db)

conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
conn.autocommit = True
cur = conn.cursor()
stmt = "insert into title_songs select title_id, song_id from title_songs_temp join title_basics using(title_id)"
cur.execute(stmt)
stmt = "drop table title_songs_temp"
cur.execute(stmt)
cur.close()
conn.close()