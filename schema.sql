CREATE TABLE test.sales(
    id int primary key auto_increment,
    order_number int,
    quantity_ordered int,
    price_each decimal(20,2),
    order_line_number int,
    sales decimal(20,2),
    order_date datetime,
    status varchar(32),
    qtr_id int,
    month_id int,
    year_id int,
    product_line varchar(64),
    msrp int,
    product_code varchar(64),
    customer_name varchar(256),
    phone varchar(32),
    address_line_1 varchar(256),
    address_line_2 varchar(256),
    city varchar(32),
    state varchar(32),
    postal_code varchar(32),
    country varchar(32),
    territory varchar(32),
    contact_last_name varchar(64),
    contact_first_name varchar(64),
    deal_size varchar(32)
);

CREATE TABLE test.covid(
Province_State varchar(32),
Country_Region varchar(32),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
date_observation datetime,
confirmed int,
deaths int,
recovered int);

CREATE TABLE test.confirmed(
Province_State varchar(100),
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
Date varchar(10),
confirmed int
);

CREATE TABLE test.deaths(
Province_State varchar(100),
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
Date varchar(10),
Deaths int
);

CREATE TABLE test.recovered(
Province_State varchar(100),
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
Date varchar(10),
Recovered int
);

CREATE TABLE test.resumen_confirmed(
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
Date date,
confirmed int
);

CREATE TABLE test.resumen_deaths(
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
Date date,
Deaths int
);

CREATE TABLE test.resumen_recovered(
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
Date date,
Recovered int
);

CREATE TABLE test.consolidado(
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
Date date,
confirmed int,
Deaths int,
Recovered int
);


CREATE TABLE test.resumen(
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6),
Date date,
confirmed int,
Deaths int,
Recovered int,
confirmed_today int,
Deaths_today int,
Recovered_today int
);

CREATE TABLE test.paises(
Country_Region varchar(100),
Lat DECIMAL(8,6),
Lon DECIMAL(9,6)
);
CREATE INDEX resumen_confirmed ON test.resumen_confirmed (Date,Lat,lon);
CREATE INDEX resumen_deaths ON test.resumen_deaths (Date,Lat,lon);
CREATE INDEX resumen_recovered ON test.resumen_recovered (Date,Lat,lon);
CREATE INDEX resumen ON test.resumen (Date, Country_Region);
CREATE INDEX consolidado ON test.consolidado (Date, Country_Region);
CREATE INDEX resumen_confirmed_ ON test.resumen_confirmed (Date,Country_Region);
CREATE INDEX resumen_deaths_ ON test.resumen_deaths (Date,Country_Region);
CREATE INDEX resumen_recovered_ ON test.resumen_recovered (Date,Country_Region);