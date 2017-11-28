\c imdb

\copy directors from C:/Users/leond/Desktop/pg/directors.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy person_basics from C:/Users/leond/Desktop/pg/person_basics.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy person_professions from C:/Users/leond/Desktop/pg/person_professions.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy principals from C:/Users/leond/Desktop/pg/principals.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy stars from C:/Users/leond/Desktop/pg/stars.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy title_basics from C:/Users/leond/Desktop/pg/title_basics.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy title_episodes from C:/Users/leond/Desktop/pg/title_episodes.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy title_genres from C:/Users/leond/Desktop/pg/title_genres.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy title_ratings from C:/Users/leond/Desktop/pg/title_ratings.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy writers from C:/Users/leond/Desktop/pg/writers.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

/*Add foreign keys after loading data*/
ALTER TABLE directors ADD FOREIGN KEY (title_id) REFERENCES title_basics(title_id);
ALTER TABLE directors ADD FOREIGN KEY (person_id) REFERENCES person_basics(person_id);
ALTER TABLE person_professions ADD FOREIGN KEY (person_id) REFERENCES person_basics(person_id);
ALTER TABLE title_genres ADD FOREIGN KEY (title_id) REFERENCES title_basics(title_id);
ALTER TABLE title_episodes ADD FOREIGN KEY (title_id) REFERENCES title_basics(title_id);
ALTER TABLE title_episodes ADD FOREIGN KEY (parent_title_id) REFERENCES title_basics(title_id);
ALTER TABLE title_ratings ADD FOREIGN KEY (title_id) REFERENCES title_basics(title_id);
ALTER TABLE stars ADD FOREIGN KEY (person_id) REFERENCES person_basics(person_id);
ALTER TABLE stars ADD FOREIGN KEY (title_id) REFERENCES title_basics(title_id);
ALTER TABLE principals ADD FOREIGN KEY (title_id) REFERENCES title_basics(title_id);
ALTER TABLE principals ADD FOREIGN KEY (person_id) REFERENCES person_basics(person_id);
ALTER TABLE writers ADD FOREIGN KEY (title_id) REFERENCES title_basics(title_id);
ALTER TABLE writers ADD FOREIGN KEY (person_id) REFERENCES person_basics(person_id);