CREATE TABLE IF NOT EXISTS STOCK (
-- SERIAL is not a true data type, but is simply shorthand notation that tells Postgres to create a auto incremented,
-- unique identifier for the specified column.
    ID serial,
    TICKET varchar(8) not null,
    AVG_VOLUME numeric(7,6),
    PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS BAR_FIVE_MINUTES (
    ID SERIAL,
    FK_STOCK_ID int not null,
    DATE timestamp not null,
    OPEN numeric(7,6),
    HIGH numeric(7,6),
    LOW numeric(7,6),
    CLOSE numeric(7,6),
    VOLUME numeric(7,6),
    OPEN_INTEREST integer,
    PRIMARY KEY(ID),
    FOREIGN KEY (fk_stock_id) REFERENCES STOCK
);