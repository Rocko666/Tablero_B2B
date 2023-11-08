+------------------------------+-----------+----------------------------------+-------+----------+
| ENTIDAD                      | PARAMETRO | VALOR                            | ORDEN | AMBIENTE |
+------------------------------+-----------+----------------------------------+-------+----------+
| D_OTC_T_B2B_PARQUE_FACTURACION | RUTA      | /RGenerator/reportes/tablero_b2b |     0 |        1 |
| D_OTC_T_B2B_PARQUE_FACTURACION | ESQUEMA   | db_reportes                      |     0 |        1 |
| D_OTC_T_B2B_PARQUE_FACTURACION | COLA_HIVE | capa_semantica                   |     0 |        1 |
+------------------------------+-----------+----------------------------------+-------+----------+

--PARAMETROS PARA LA ENTIDAD D_OTC_T_B2B_PARQUE_FACTURACION
DELETE FROM params_des WHERE entidad='D_OTC_T_B2B_PARQUE_FACTURACION';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','RUTA','/home/nae108834/RGenerator/reportes/tablero_b2b','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','ESQUEMA','db_desarrollo2021','0','0'); --db_reportes
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','COLA_HIVE','capa_semantica','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_OTC_T_B2B_PARQUE_FACTURACION';

--PARAMETROS PARA LA ENTIDAD D_OTC_T_B2B_PARQUE_FACTURACION
DELETE FROM params_des WHERE entidad='D_OTC_T_B2B_PARQUE_FACTURACION';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','SHELL','/home/nae108834/RGenerator/reportes/tablero_b2b/bin/otc_t_b2b_carga.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','VAL_RUTA','/home/nae108834/RGenerator/reportes/tablero_b2b','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','VAL_QUEUE','capa_semantica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','VAL_ESQUEMA','db_desarrollo2021','0','0'); --db_reportes
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','VAL_MASTER','yarn','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','VAL_DRIVER_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','VAL_EXECUTOR_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','VAL_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_B2B_PARQUE_FACTURACION','VAL_NUM_EXECUTOR_CORES','8','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_OTC_T_B2B_PARQUE_FACTURACION';
