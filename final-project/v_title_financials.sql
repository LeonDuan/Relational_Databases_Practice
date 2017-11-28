# what are the movies whose box office is more than twice its budget (only considering movies with $100,000,000+ budget), ordered by box_office
create view v_title_financials as
select primary_title, box_office, budget
from title_basics
join title_financials using(title_id)
where box_office > 2 * budget and budget > 100000000;