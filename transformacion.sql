DELIMITER //
CREATE PROCEDURE TRANSFORMACION ()
BEGIN
insert into resumen_confirmed
select Country_Region, max(lat) as lat, max(lon) as lon, str_to_date(Date, '%m/%e/%y') as Date, sum(confirmed) confirmed
from confirmed
group by Country_Region, str_to_date(Date, '%m/%e/%y');

insert into resumen_deaths
select Country_Region, max(lat) as lat, max(lon) as lon, str_to_date(Date, '%m/%e/%y') as Date, sum(Deaths) as Deaths
from deaths
group by Country_Region, str_to_date(Date, '%m/%e/%y');

insert into resumen_recovered
select Country_Region, max(lat) as lat, max(lon) as lon, str_to_date(Date, '%m/%e/%y') as Date, sum(IFNULL(Recovered,0)) as Recovered
from recovered
group by Country_Region, str_to_date(Date, '%m/%e/%y');

DELETE FROM consolidado where 1=1;

insert into consolidado
select  c.Country_Region, c.Lat, c.Lon, c.Date, IFNULL(c.confirmed,0) confirmed,
	IFNULL(d.Deaths,0) Deathds,IFNULL(r.Recovered,0) Recovered
from resumen_confirmed c left join resumen_deaths d on
	c.Date = d.Date and
    c.Country_Region = d.Country_Region
left join resumen_recovered r on
	c.Date = r.Date and
    c.Country_Region = r.Country_Region;


DELETE FROM resumen where 1=1;

insert into resumen
SELECT R1.Country_Region, R1.Lat, R1.Lon, R1.Date,
R1.confirmed as confirmed ,
R1.Deaths as Deaths,
R1.Recovered as Recovered,
case when IFNULL(R1.confirmed,0)-IFNULL(R2.confirmed,0)<0 then 0 else IFNULL(R1.confirmed,0)-IFNULL(R2.confirmed,0) end as confirmed_today,
case when IFNULL(R1.Deaths,0)-IFNULL(R2.Deaths,0)<0 then 0 else IFNULL(R1.Deaths,0)-IFNULL(R2.Deaths,0)  end as deaths_today,
case when IFNULL(R1.Recovered,0)-IFNULL(R2.Recovered,0)<0 then 0 else IFNULL(R1.Recovered,0)-IFNULL(R2.Recovered,0) end as recovered_today
FROM consolidado R1 JOIN consolidado R2 ON
	R1.Date = date_add(R2.Date,INTERVAL +1 DAY) and
    R1.Country_REgion = R2.Country_Region  ;

update resumen
set confirmed_today=confirmed,
	Deaths_today=Deaths,
    recovered_today=recovered
where date=(select min(date) from consolidado);

DELETE FROM paises where 1=1;

insert into paises
select Country_Region, lat, lon
from resumen
group by Country_Region, lat, lon;

END
//
DELIMITER ;

