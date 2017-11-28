# title_songs foreign keys
alter table title_songs add foreign key (title_id) references title_basics(title_id);
alter table title_songs add foreign key (song_id) references songs(song_id);

# singer_songs foreign keys
alter table singer_songs add foreign key (person_id) references person_basics(person_id);
alter table singer_songs add foreign key (song_id) references songs(song_id);