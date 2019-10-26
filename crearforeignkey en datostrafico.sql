Use proyecto;

ALTER TABLE DatosTrafico
ADD CONSTRAINT FK_SensorTraficor
FOREIGN KEY (id_sensor) REFERENCES SensoresTrafico(id);