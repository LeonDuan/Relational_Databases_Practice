\c dev;
drop database if exists imdb;
create database imdb;
\c imdb;

CREATE TABLE IF NOT EXISTS directors (
  title_id varchar(10),
  person_id varchar(30),
  PRIMARY KEY (title_id, person_id)
);


CREATE TABLE IF NOT EXISTS person_basics (
  person_id varchar(10),
  primary_name varchar(110),
  birth_year int,
  death_year int,
  PRIMARY KEY (person_id)
);


CREATE TABLE IF NOT EXISTS person_professions (
  person_id varchar(10),
  profession varchar(30),
  PRIMARY KEY (person_id, profession)
);


CREATE TABLE IF NOT EXISTS principals (
  title_id varchar(10),
  person_id varchar(10),
  PRIMARY KEY (title_id, person_id)
);


CREATE TABLE IF NOT EXISTS stars (
  person_id varchar(10),
  title_id varchar(10),
  PRIMARY KEY (person_id, title_id)
);

CREATE TABLE IF NOT EXISTS title_basics (
  title_id varchar(10),
  title_type varchar(20),
  primary_title varchar(300),
  original_title varchar(300),
  is_adult boolean,
  start_year int,
  end_year int,
  runtime_minutes int,
  PRIMARY KEY (title_id)
);


CREATE TABLE IF NOT EXISTS title_episodes (
  title_id varchar(10),
  parent_title_id varchar(10),
  season_num int,
  episode_num int,
  PRIMARY KEY (title_id)
);


CREATE TABLE IF NOT EXISTS title_genres (
  title_id varchar(10),
  genre varchar(20),
  PRIMARY KEY (title_id, genre)
);

CREATE TABLE IF NOT EXISTS title_ratings (
  title_id varchar(10),
  average_rating float,
  num_votes int,
  PRIMARY KEY (title_id)
);

CREATE TABLE IF NOT EXISTS writers (
  title_id varchar(10),
  person_id varchar(10),
  PRIMARY KEY (title_id, person_id)
);


