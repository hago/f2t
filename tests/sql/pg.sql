create table if not exists demo (
    id serial not null,
    "name" varchar(100),
    "description" text,
    isdead bool,
    age smallint,
    "number" int,
    long bigint,
    floatnumber float,
    doublenumber double precision,
    decimaldefault decimal,
    decimalnumber decimal(20,5),
    tswz timestamp with time zone,
    tsoz timestamp without time zone,
    dt date,
    tm time,
    primary key(id, "name"),
    unique("name", age),
    unique("number", age, long)
);

create table if not exists partdemo (
    id int not null,
    primary key(id)
) PARTITION BY RANGE (id);

create table if not exists partdemo_below_100 partition of partdemo
for values from (MINVALUE) to (100);
create table if not exists partdemo_100_and_above partition of partdemo
for values from (100) to (MAXVALUE);

