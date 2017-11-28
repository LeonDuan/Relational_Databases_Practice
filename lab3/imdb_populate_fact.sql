/* title_type, year, genre, appalling_titles */
select title_type, start_year as year, genre, count(*) as appalling_titles
from title_genres join (
	select * from title_basics join title_ratings using(title_id)
	where average_rating <= 2.0) sub using(title_id)
group by title_type, start_year, genre;

/* title_type, year, genre, average_titles */
select title_type, start_year as year, genre, count(*) as average_titles
from title_genres join (
	select * from title_basics join title_ratings using(title_id)
	where 2.1 <= average_rating and average_rating <= 7.9) sub using(title_id)
group by title_type, start_year, genre;

/* title_type, year, genre, outstanding_titles */
select title_type, start_year as year, genre, count(*) as outstanding_titles
from title_genres join (
	select * from title_basics join title_ratings using(title_id)
	where average_rating >= 8.0) sub using(title_id)
group by title_type, start_year, genre;

/* Create table Title_Rating_Facts_Appalling */
create table Title_Rating_Facts_Appalling
as (select title_type, start_year as year, genre, count(*) as appalling_titles
	from title_genres join (
		select * from title_basics join title_ratings using(title_id)
		where average_rating <= 2.0) sub using(title_id)
	group by title_type, start_year, genre);

/* Create table Title_Rating_Facts_Average */
create table Title_Rating_Facts_Average
as (select title_type, start_year as year, genre, count(*) as average_titles
	from title_genres join (
		select * from title_basics join title_ratings using(title_id)
		where 2.1 <= average_rating and average_rating <= 7.9) sub using(title_id)
	group by title_type, start_year, genre);

/* Create table Title_Rating_Facts_Outstanding */
create table Title_Rating_Facts_Outstanding
as (select title_type, start_year as year, genre, count(*) as outstanding_titles
	from title_genres join (
		select * from title_basics join title_ratings using(title_id)
		where average_rating >= 8.0) sub using(title_id)
	group by title_type, start_year, genre);

/* Create table Title_Rating_Facts */
create table Title_Rating_Facts
as (select * from (
		select * from Title_Rating_Facts_Appalling
		full join Title_Rating_Facts_Average using(title_type, year, genre)) sub
	full join Title_Rating_Facts_Outstanding using(title_type, year, genre));

/* Update NULLs to 0 */
update Title_Rating_Facts
set appalling_titles = 0
where appalling_titles is NULL;

update Title_Rating_Facts
set average_titles = 0
where average_titles is NULL;

update Title_Rating_Facts
set outstanding_titles = 0
where outstanding_titles is NULL;

/* Deleting rows to make primary key */
delete from Title_Rating_Facts
	where title_type is NULL or year is NULL or genre is NULL;

alter table Title_Rating_Facts
add primary key	(title_type, year, genre);

/* aggregating over year and genre with restrictions */
create view v_outstanding_titles_by_year_genre
as (select year, genre, sum(outstanding_titles) sum_outstanding
	from Title_Rating_Facts
	group by year, genre
	having year >= 1930 and sum(outstanding_titles) > 0
	order by year, genre
	limit 100);
