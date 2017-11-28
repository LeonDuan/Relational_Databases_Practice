# top 50 movies that have the greatest number of tags
create materialized view v_title_tags as
select primary_title, count(*) as tag_count
from title_tags
join title_basics using (title_id)
group by primary_title
order by count(*) desc
limit 50;