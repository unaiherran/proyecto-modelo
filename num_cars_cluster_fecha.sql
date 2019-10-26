use proyecto;

SELECT num_cars
, clu.id_cluster
, cam.id_camara
, ima.fecha

from ImagenesCamarasTrafico ima
INNER JOIN CamarasTrafico cam
ON ima.id_camara = cam.id_camara

inner join Cluster clu
on cam.Cluster = clu.id_cluster
#order by fecha

where ima.fecha BETWEEN str_to_date('2019-10-01', '%Y-%m-%d') AND '2019-10-21'
;

