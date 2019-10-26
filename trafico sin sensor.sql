use proyecto;

select distinct id_sensor from DatosTrafico; #4074
select id from SensoresTrafico; #4135

SELECT id_sensor FROM DatosTrafico WHERE  id_sensor NOT IN (SELECT ID FROM SensoresTrafico) group by id_Sensor;