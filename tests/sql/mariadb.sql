CREATE OR REPLACE TABLE demo (
    id serial not null,
    name varchar(100),
    description text,
    isdead bool,
    age smallint,
    number int,
    long bigint,
    floatnumber float,
    doublenumber double precision,
    decimaldefault decimal,
    decimalnumber decimal(20,5),
    tswz timestamp with time zone,
    tsoz timestamp without time zone,
    primary key(id, "name"),
    unique("name", age),
    unique("number", age, long)
);
