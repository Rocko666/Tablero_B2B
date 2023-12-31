--PARAMETROS PARA LA ENTIDAD OTC_T_B2B_PARQUE_FACTURACION
DELETE FROM params WHERE entidad='OTC_T_B2B_PARQUE_FACTURACION';
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','SHELL','/RGenerator/reportes/tablero_b2b/bin/otc_t_b2b_carga.sh','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','VAL_RUTA','/RGenerator/reportes/tablero_b2b','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','VAL_QUEUE','capa_semantica','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','VAL_ESQUEMA','db_reportes','0','1'); 
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','VAL_MASTER','yarn','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','VAL_DRIVER_MEMORY','16G','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','VAL_EXECUTOR_MEMORY','16G','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','VAL_NUM_EXECUTORS','8','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_B2B_PARQUE_FACTURACION','VAL_NUM_EXECUTOR_CORES','8','0','1');
SELECT * FROM params WHERE ENTIDAD='OTC_T_B2B_PARQUE_FACTURACION';
