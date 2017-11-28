/*who are all the stars that appeared in titles started in the 80s*/
SELECT DISTINCT(person_basics.primary_name)
FROM person_basics 
JOIN stars using (person_id)
JOIN title_genres on stars.title_id = title_genres.title_id
JOIN title_basics on title_genres.title_id = title_basics.title_id
WHERE genre = 'Horror' and title_basics.start_year >= 1980 and title_basics.end_year <= 1989;

/*what are the top 5 titles directed by Christopher Nolan*/
SELECT primary_title, average_rating
FROM title_basics
JOIN directors USING (title_id)
JOIN person_basics USING (person_id)
JOIN title_ratings on directors.title_id = title_ratings.title_id
WHERE person_basics.primary_name = 'Christopher Nolan'
ORDER BY average_rating DESC
LIMIT 5;

/*what is the shortest adult-only movie*/
SELECT primary_title, runtime_minutes
FROM title_basics
WHERE is_adult = TRUE and title_type = 'movie'
ORDER BY runtime_minutes
LIMIT 1;

/*what are the top 10 comedy tv series with the most episode (any season)*/
SELECT primary_title, MAX(episode_num) as num_of_episodes
FROM title_basics
JOIN title_episodes on title_basics.title_id = title_episodes.parent_title_id
JOIN title_genres on title_episodes.parent_title_id = title_genres.title_id
WHERE episode_num > 50 and title_type = 'tvSeries' and genre = 'Comedy'
GROUP BY primary_title
ORDER BY num_of_episodes DESC;

/*who the movies whose original title is different from its primary title, sorted from oldest to newest*/
SELECT primary_title, original_title
FROM title_basics
WHERE primary_title != original_title
ORDER BY start_year;

/*which titles have runtimes over 1000 minutes (and aren't typos)*/
select distinct(primary_title), runtime_minutes, start_year, end_year
from title_genres 
join title_basics using(title_id)
where runtime_minutes > 1000 and start_year is not NULL and end_year is not NULL

/*what is the longest runtime in each genre*/
select max(runtime_minutes), genre
from (select *
      from title_genres join title_basics using(title_id)
      where start_year is not NULL and end_year is not NULL)
group by genre
order by genre

/*who are the people that have more than one professions*/
select distinct(t1.primary_name)
from 
(select primary_name, person_basics.person_id, profession
 from person_basics
 join person_professions using(person_id)
) as t1
join
(select primary_name, person_basics.person_id, profession
 from person_basics
 join person_professions using(person_id)
) as t2
on t1.profession != t2.profession and t1.person_id = t2.person_id;

/*who are the top five worst movies with Leonardo Dicaprio in it as a principal, sorted by average rating*/
select title_basics.primary_title, title_ratings.average_rating
from person_basics
join principals on person_basics.person_id = principals.person_id
join title_ratings on principals.title_id = title_ratings.title_id
join title_basics on title_ratings.title_id = title_basics.title_id
where person_basics.primary_name = 'Leonardo DiCaprio'  and title_basics.title_type = 'movie'
order by title_ratings.average_rating 
limit 5

/*how many movies are in each genre*/
select count(*), title_genres.genre
from title_basics
join title_genres using(title_id)
where title_basics.title_type = 'movie'
group by title_genres.genre

/*people who died in the same year he/she appeared in any show that started that year*/
select primary_name
from person_basics
join stars using(person_id)
join title_basics using (title_id)
where person_basics.death_year = title_basics.start_year

/*who are the writers who are once also a star and once also a director, sorted by name in alphabetical order*/
select distinct(primary_name)
from person_basics
join directors using(person_id)
join writers on directors.person_id = writers.person_id
join stars on writers.person_id = stars.person_id

/*which titles had more than one genre*/
select title_id, count(*)
from title_genres
group by title_id
having count(*)>1

/*who directed the most titles*/
select primary_name, number_of_titles
from (select person_id, count(*) as number_of_titles
from directors
group by person_id
order by count(*) desc
limit 1) join person_basics using (person_id)

/*what is the title(s) with the longest runtime in each genre*/
select genre, primary_title, runtime_minutes
from (select genre, max(runtime_minutes) as max_runtime
      from (select *
           from title_genres join title_basics using(title_id)
           where start_year is not NULL and end_year is not NULL)
      group by genre)
join title_basics on max_runtime = runtime_minutes

/*which principals also directed movies*/
select primary_name
from (select distinct principals.person_id as p1
     from principals join directors using(person_id))
     join person_basics on person_basics.person_id = p1
order by primary_name asc

/*who are the top 50 principals in movies with the highest average movie rating (from over 1000 votes), order by the rating*/
select AVG(title_ratings.average_rating) as Average_Rating, primary_name
from title_ratings
join title_basics on title_ratings.title_id = title_basics.title_id and title_basics.title_type = 'movie' and title_ratings.num_votes > 1000
join principals on principals.title_id = title_basics.title_id
join person_basics on principals.person_id = person_basics.person_id
GROUP BY primary_name
order by Average_Rating DESC
limit 50

/*what are 20 lowest rating tv series*/
select primary_title, average_rating
from title_basics
join title_ratings on (title_basics.title_type = 'tvSeries') and (title_basics.title_id = title_ratings.title_id) and (title_ratings.num_votes > 1000)
order by title_ratings.average_rating
limit 20

/*what is the average rating by genre, sorted from high to low*/
select AVG(average_rating) as Average_Rating, title_genres.genre
from title_ratings
join title_genres on title_ratings.title_id = title_genres.title_id
group by title_genres.genre
order by Average_Rating DESC

/*who are the top 10 stars who have been in comedy movies, sorted by number of appereance*/
select primary_name, count(*) as count
from person_basics
join stars using(person_id)
join title_genres on stars.title_id = title_genres.title_id
join title_basics on title_genres.title_id = title_basics.title_id and title_basics.title_type = 'movie'
where genre = 'Comedy'
group by primary_name
order by count desc
limit 10