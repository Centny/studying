drop table if exists dms_user;

create table dms_user(
    tid varchar not null primary key,
    username varchar,
    password varchar,
    status int
);

insert into dms_user values("_0","u0","p0",100);