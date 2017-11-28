CREATE EXTERNAL TABLE IF NOT EXISTS imdb.directors (
  `title_id` string,
  `person_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/directors/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.person_basics (
  `person_id` string,
  `primary_name` string,
  `birth_year` int,
  `death_year` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/person_basics/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.person_professions (
  `person_id` string,
  `profession` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/person_professions/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.principals (
  `title_id` string,
  `person_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/principals/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.stars (
  `person_id` string,
  `title_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/stars/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.title_basics (
  `title_id` string,
  `title_type` string,
  `primary_title` string,
  `original_title` string,
  `is_adult` boolean,
  `start_year` int,
  `end_year` int,
  `runtime_minutes` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/title_basics/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.title_episodes (
  `title_id` string,
  `parent_title_id` string,
  `season_num` int,
  `episode_num` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/title_episodes/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.title_genres (
  `title_id` string,
  `genre` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/title_genres/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.title_ratings (
  `title_id` string,
  `average_rating` float,
  `num_votes` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/title_ratings/'
TBLPROPERTIES ('has_encrypted_data'='false')

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.writers (
  `title_id` string,
  `person_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/writers/'
TBLPROPERTIES ('has_encrypted_data'='false')
