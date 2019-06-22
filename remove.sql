REVOKE ALL PRIVILEGES ON car, driver, route, route_pt FROM ivanov, petrov, sokolov, kozlov CASCADE;
DROP USER ivanov, petrov, sokolov, kozlov;
DROP TABLE car, driver, route, route_pt, gps_pt CASCADE;
DROP FUNCTION IF EXISTS driver_change(
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
    condition   text);
DROP FUNCTION IF EXISTS trg_i_gps_pt();
DROP TRIGGER IF EXISTS i_gps_pt ON gps_pt;
DROP ROLE operators, dispatchers;
