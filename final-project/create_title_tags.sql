create table Title_Tags (title_id varchar(10), tag varchar(300));
alter table title_tags add constraint title_tags_pkey primary key (title_id, tag);