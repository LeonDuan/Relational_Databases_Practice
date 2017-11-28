# for each genre, what is the greatest discrepancy between the imdb rating and the movielens rating (only look at movies with 3000+ ratings)
create view v_movielens_rating as
select genre, abs(max(average_rating - movielens_rating * 2)) as difference
from title_basics
join title_ratings using(title_id)
join title_genres on title_basics.title_id = title_genres.title_id
where num_votes > 3000
group by genre
order by abs(max(average_rating - movielens_rating * 2)) desc;