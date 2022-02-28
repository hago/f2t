CREATE OR REPLACE TABLE demo (
    id bigint not null,
    code char(20),
    name varchar(100),
    description text,
    isdead bool,
    age smallint,
    age2 mediumint,
    number int,
    longint bigint,
    floatnumber float,
    doublenumber double,
    decimaldefault decimal,
    decimalnumber decimal(20,5),
    dtts timestamp,
    dtd date,
    dtt time,
    dt datetime,
    primary key(id, name),
    unique(name, age),
    unique(number, age, longint)
);