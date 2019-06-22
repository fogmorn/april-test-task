-- 2. Создание ролей

CREATE ROLE operators;
CREATE ROLE dispatchers;

CREATE USER ivanov;
CREATE USER petrov;
CREATE USER sokolov;
CREATE USER kozlov;
--GRANT ALL PRIVILEGES ON database TO ivanov;
GRANT operators TO petrov, kozlov;
GRANT dispatchers TO petrov, sokolov;



-- 3. Создание справочников

CREATE TABLE car (
    gosnum      varchar(9) NOT NULL UNIQUE,
    vin         char(17) UNIQUE PRIMARY KEY,
    mark        varchar(25) NOT NULL
);


CREATE TABLE driver (
    id        SERIAL PRIMARY KEY,
    lastnm    varchar(20) NOT NULL,
    firstnm   varchar(20) NOT NULL,
    secnm     varchar(20) NOT NULL,
    b_dt      date,
    sex       char(1),
    rec_dt    date,
    status    varchar(11) DEFAULT 'не работает',
    driver_lic varchar(5),
    address   varchar(80),
    phone     char(11),
    UNIQUE (lastnm, firstnm, secnm)
);

CREATE TABLE route (
    id          SERIAL PRIMARY KEY,
    car_id      char(17) NOT NULL,
    route_type  varchar(25) NOT NULL DEFAULT 'Прямая дорога',
    status      varchar(9) NOT NULL DEFAULT 'не открыт',
    datetime    timestamp NOT NULL,
    user_id     int NOT NULL,
    driverid    int NOT NULL
);

CREATE TABLE route_pt (
    id          SERIAL PRIMARY KEY,
    route_id    int NOT NULL,
    obj_id      int NOT NULL,
    obj_type    varchar(10) NOT NULL,
    obj_name    varchar(25) NOT NULL,
    arr_plan    timestamp NOT NULL,
    arr_fact    timestamp NOT NULL,
    dep_plan    timestamp NOT NULL,
    dep_fact    timestamp NOT NULL,
    CONSTRAINT valid_plan CHECK (arr_plan < dep_plan),
    CONSTRAINT valid_fact CHECK (arr_fact < dep_fact)
);

GRANT UPDATE ON car TO dispatchers;
GRANT SELECT ON car TO PUBLIC;

GRANT UPDATE ON driver TO dispatchers;
GRANT SELECT ON driver TO operators, dispatchers;

GRANT UPDATE ON route TO dispatchers;
GRANT SELECT ON route TO operators, dispatchers;

GRANT UPDATE ON route_pt TO dispatchers;
GRANT SELECT ON route_pt TO operators, dispatchers;


-- 4. Заполнение таблиц

INSERT INTO car (gosnum, vin, mark) 
VALUES ('А593БН93', 'WVGZZZ7LZ4D071136', 'Wagen');
INSERT INTO car (gosnum, vin, mark) 
VALUES ('Х678ЕН123', 'SKGZPP7LZ4D074472', 'Skoda');


INSERT INTO driver (
lastnm, firstnm, secnm, b_dt, sex, rec_dt, status, driver_lic, address, phone)
VALUES (
'Маркарян', 'Акоп', 'Хоренович', '24.09.1983', 'm', '08.10.2013', 'работает', 
'BC', 'Краснодар, Туляева, 8/235', '79833836743');
INSERT INTO driver (
lastnm, firstnm, secnm, b_dt, sex, rec_dt, status, driver_lic, address, phone)
VALUES (
'Кравченко', 'Юрий', 'Петрович', '13.11.1980', 'm', '23.06.2010', 'работает', 
'BCDE', 'Краснодар, Московская, 13/1', '79181723452');


INSERT INTO route (car_id, datetime, user_id, driverid)
VALUES (
'SKGZPP7LZ4D074472', '2016-09-18 17:51:47', '15', '2');


INSERT INTO route_pt
(route_id, obj_id, obj_type, obj_name, arr_plan, arr_fact, dep_plan, dep_fact)
VALUES (1, 4, 2, 'Склад 4', '2016-10-13 08:10:00', '2016-10-13 08:08:57', 
'2016-10-13 08:40:00', '2016-10-13 08:40:07');


UPDATE car SET gosnum='К452РР05' WHERE vin='WVGZZZ7LZ4D071136';
UPDATE driver SET address='Краснодар, Садовая, 9/2' 
WHERE id=1;


-- 6. Хранимая процедура

CREATE FUNCTION driver_change(
    lastnm      text DEFAULT NULL,
    firstnm     text DEFAULT NULL,
    secnm       text DEFAULT NULL,
    b_dt        date DEFAULT NULL,
    sex         text DEFAULT NULL,
    rec_dt      date DEFAULT NULL,
    status      text DEFAULT NULL,
    driver_lic  text DEFAULT NULL,
    address     text DEFAULT NULL,
    phone       text DEFAULT NULL,
    action      text DEFAULT NULL,
    condition   text DEFAULT NULL)
    RETURNS text
AS $$
if phone:
    if len(phone) <> 11:
        return 'Phone number must be equal "11" characters!'
    if phone[0] <> '7':
        return 'The valid phone number format is: 79181112233\n\'%s\'' % phone


if action == 'insert':
    if (lastnm is None) or (firstnm is None) or (secnm is None):
        return 'ERROR: FIO is the obligatory field!'
    try:
        qry = plpy.prepare("""
	    INSERT INTO driver (
                lastnm, firstnm, secnm, b_dt, sex, rec_dt, 
                status, driver_lic, address, phone)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)""", 
            ["text", "text", "text", "date", "text", "date", 
             "text", "text", "text", "text"])
        plpy.execute(qry, 
            [lastnm, firstnm, secnm, b_dt, sex, rec_dt, status, 
             driver_lic, address, phone])
    except plpy.SPIError, e:
        return "Error while add data: %s" % e.sqlstate
    return 'Success!'
elif action == 'update':
    arg_lst = ["lastnm", "firstnm", "secnm", "b_dt", "sex", 
               "rec_d", "status", "driver_lic", "address", "phone"]
    type_lst = ["text", "text", "text", "date", "text", 
                "date", "text", "text", "text", "text"]
    j = 0
    k = 0
    vars = ''
    q_str = '' 

    for i in range(0, 10):
        if args[i] is not None:
            k+=1
            q_str += '%s=$%d, ' % (arg_lst[j], k) 
            vars += '%s, ' % args[i]
        else:
           type_lst.pop(k)
        j+=1

    q_str = q_str.rstrip(', ')
    vars = vars.rstrip(', ')
    vars = vars.split(', ')

    try:
        qry = plpy.prepare("UPDATE driver SET %s WHERE %s" % (q_str, condition), type_lst)
        plpy.execute(qry, vars)
    except plpy.SPIError, e:
        return "ERROR: %s" % e.sqlstate
    return 'Success!'
else:
    return 'Action is not recognized as permissible!'
$$ LANGUAGE plpythonu;

GRANT EXECUTE ON FUNCTION driver_change(
    lastnm      text,
    firstnm     text,
    secnm       text,
    b_dt        date,
    sex         text,
    rec_dt      date,
    status      text,
    driver_lic  text,
    address     text,
    phone       text,
    action      text,
    condition   text
) TO dispatchers;


-- 7. Представление

CREATE VIEW route_points AS SELECT mark, gosnum, 
    CONCAT_WS(' ', lastnm, firstnm, secnm) AS fio, datetime, rpt.id, 
    CONCAT_WS(' ', obj_type, obj_name) AS object, 
    CONCAT_WS('/', arr_plan, dep_plan) AS arrdep_plan,
    CONCAT_WS('/', arr_fact, dep_fact) AS arrdep_fact
FROM route_pt rpt
LEFT JOIN route r ON rpt.route_id=r.id
LEFT JOIN driver d ON r.driverid=d.id
LEFT JOIN car c ON r.car_id=c.vin;

GRANT SELECT ON route_points TO operators, dispatchers;


-- 8. Хранение GPS-точек

CREATE TABLE gps_pt (
    id          SERIAL PRIMARY KEY,
    car_id      char(17) NOT NULL,
    datetime    timestamp NOT NULL,
    lon         numeric(9,6) NOT NULL,
    lat         numeric(9,6) NOT NULL,
    alt         varchar(4) NOT NULL,
    spd         varchar(3) NOT NULL
);

GRANT SELECT ON gps_pt TO dispatchers;
GRANT UPDATE ON gps_pt TO operators;


-- 9. Триггер

CREATE FUNCTION trg_i_gps_pt() RETURNS TRIGGER AS $$
from datetime import datetime, timedelta
dt =  TD["new"]["datetime"]
lon = int(TD["new"]["lon"])
lat = int(TD["new"]["lat"])
alt = int(TD["new"]["alt"])
spd = int(TD["new"]["spd"])

dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')

if dt-datetime.now() > timedelta(hours=48):
    return 'SKIP'
if lon < -180.0 or lon > 180.0:
    return 'SKIP'
if lat < -90.0 or lat > 90.0:
    return 'SKIP'
if alt < 0 or alt > 5000:
    return 'SKIP'
if spd < 0 or spd > 150:
    return 'SKIP'
return 'OK'
$$ LANGUAGE plpythonu;


CREATE TRIGGER i_gps_pt
BEFORE INSERT ON gps_pt
FOR EACH ROW
EXECUTE PROCEDURE trg_i_gps_pt();
