# who are the singers who have sung songs over 8 minutes long, order by the number of such songs they have sung
create view v_m4_singers_with_10_plus_minute_songs as
select primary_name, count(*) as count
from person_basics
join singer_songs using(person_id)
join songs using(song_id)
where song_duration > 8
group by primary_name
order by count(*) desc;

# movies with 15 or more songs
create view v_m4_movies_with_15_plus_songs as 
select primary_title, o.count
from title_basics join
(select title_id, count(*) as count
from title_basics
join title_songs using(title_id)
group by title_id
having count(*) >= 15) o
using(title_id)
order by o.count desc;
