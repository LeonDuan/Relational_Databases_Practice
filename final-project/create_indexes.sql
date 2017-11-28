# create an index with primary title (capitalized) and start year for the first select query in my code
create index ptc_sy_idx on title_basics(upper(primary_title), start_year);

# create an index with primary title (capitalized), start year, and title type (partial) for the second select query in my code
create index ptc_sy_tt_idx on title_basics(upper(primary_title), start_year, title_type) where title_type <> 'tvEpisode';

# I was going to make title_genre(title_id, genre) the third index, but this is already the PK of title_genres, so this pair is already indexed.