create table Songs (song_id varchar(10), song_title varchar(75), song_duration float); 
create table Title_Songs(title_id varchar(10), song_id varchar(10)); 
create table Singer_Songs(person_id varchar(10), song_id varchar(10));

alter table Songs add primary key (song_id);
alter table Title_Songs add primary key (title_id, song_id);
alter table Singer_Songs add primary key (person_id, song_id);

# add a gender column to the person_basics table
alter table person_basics add column gender varchar(1);