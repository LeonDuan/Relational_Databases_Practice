/*who are the directors whose profession is an editor that have directed more than 20 movies, descending order by # or movies (need M-view)*/
select primary_name, count(*)
from person_basics
join directors using(person_id)
join title_basics on directors.title_id = title_basics.title_id
join person_professions on person_basics.person_id = person_professions.person_id
where title_basics.title_type = 'movie' and person_professions.profession = 'editor'
group by primary_name
having count(*) > 20
order by count(*) desc;

/* top 20 years in which the most people who are a writer, a star, and a principal were born, order by number of people */
select birth_year, count(*) from
(select distinct person_id
from writers) t1
join
(select distinct person_id
from stars) t2 using(person_id)
join
(select distinct person_id
from principals) t3 using(person_id)
join person_basics using(person_id)
group by birth_year
order by count(*) desc
limit 20;

/* what TV show with over 200 episodes has the most episodes (including all seasons) (need M-view)*/
select primary_title, count(*)
from title_basics
join title_episodes on title_basics.title_id = title_episodes.parent_title_id
group by primary_title
having count(*) > 200
order by count(*) desc;

/*what is the average rating by genre (only showing those > 7), sorted from high to low*/
select AVG(average_rating), title_genres.genre
from title_ratings
join title_genres on title_ratings.title_id = title_genres.title_id
group by title_genres.genre
having AVG(average_rating) > 7
order by AVG(average_rating) DESC;

/* what is the average number of votes in each year after 2000*/
select start_year, avg(num_votes) as average
from title_ratings right join title_basics using (title_id)
where start_year > 2000
group by start_year
order by start_year;

/* how many titles are in each genre (including nulls) (need M-view)*/
select genre, count(*)
from title_basics left join title_genres using(title_id)
group by genre
order by count(*) desc;
