import psycopg2
import sys, os, configparser, csv
from pyspark import SparkConf, SparkContext

log_path = "/home/hadoop/logs/" # don't change this
aws_region = "us-east-1"  # don't change this
s3_bucket = "cs327e-fall2017-final-project" # don't change this
the_numbers_files = "s3a://" + s3_bucket + "/the-numbers/*" # dataset for milestone 3

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
  
################## process the-numbers dataset #################################

def parse_line(line):
    line = line.strip()
    if line.split()[0] == "Release": return ()

    arr = line.split('\t')

    #take care of year
    arr[0] = int(arr[0].split("/")[2])

    #take care of title
    arr[1] = arr[1].strip().upper().encode('utf-8')

    #take care of genre
    arr[2] = arr[2].strip()
    if 'Thriller/Suspense' == arr[2]:
        arr[2] = "Thriller"
    elif "Black Comedy" == arr[2]:
        arr[2] = "Comedy"
    elif "Romantic Comedy" == arr[2]:
        arr[2] = "Romance"

    #take care of budget
    arr[3] = arr[3].replace('$','').replace('\"','').replace(',','').strip()
    arr[3] = -1 if arr[3].isspace() or arr[3] == "" else int(arr[3])

    #take care of box office
    arr[4] = arr[4].replace('$','').replace('\"','').replace(',','').strip()
    arr[4] = -1 if arr[4].isspace() or arr[4] == "" else int(arr[4])

    return tuple(arr)

init() 
base_rdd = sc.textFile(the_numbers_files)
mapped_rdd = base_rdd.map(parse_line) 
print_rdd(mapped_rdd, "mapped_rdd")

def save_to_db(list_of_tuples):
  
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
    conn.autocommit = True
    cur = conn.cursor()

    # add logic to look up the title_id in the database as specified in step 5 of assignment sheet
    for t in list_of_tuples:
        if len(t) is not 5: continue

        year =  t[0] #int
        movie = t[1] #str
        genre = t[2] #str
        budget = t[3] #int
        box_office = t[4] #int

        query = "select title_id from title_basics where upper(primary_title) = %s and start_year = %s"
        cur.execute(query,(movie, year))
        rows = cur.fetchall()

        if len(rows) == 1:
            title_id = rows[0][0]
            query = "insert into title_financials values (%s, %s, %s)"
            if(title_id == 'tt2091111'):
                print(t,'\n',rows)

            try:
                cur.execute(query,(title_id, budget, box_office))
            except Exception as e:
                print("1cannot insert record", title_id, budget, box_office, e.message);
        

        else:
            if box_office > 0:
                query = "select title_id from title_basics where upper(primary_title) = %s and start_year = %s and title_type <> 'tvEpisode'"
                cur.execute(query,(movie, year))
            else:
                query = "select title_id from title_basics join title_genres using(title_id) where upper(primary_title) = %s and start_year = %s and genre = %s"
                cur.execute(query,(movie, year, genre))

            rows = cur.fetchall()

            if len(rows) == 1: #insert a record ONLY IF we can locate a single title_id 
                title_id = rows[0][0]
            else:
                continue
            query = "insert into title_financials values (%s, %s, %s)"
            if(title_id == 'tt2091111'):
                print(t,'\n',rows)
            try:
                cur.execute(query,(title_id, budget, box_office))
            except Exception as e:
                print("2cannot insert record", title_id, budget, box_office, e.message);
        

    # add logic to write out the financial record to the database as specified in step 5 of assignment sheet
   
    cur.close()
    conn.close()
  
  
mapped_rdd.foreachPartition(save_to_db)

# free up resources
sc.stop() 