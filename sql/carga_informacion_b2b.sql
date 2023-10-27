--#################################################################
--# Proyecto:          Tablas finales tablero B2B                 #
--# Cliente:           Telefonica                                 #
--# Elaborado por:     Softconsulting S.A.                        #
--# Script:            carga_informacion_b2b.sql                  #
--# Fecha de creacion: 2020/07/18                                 #
--# Fecha de modificacion:                                        #
--#################################################################
--------------------------------------------------------------
--Parametros de Entrada de La Consulta
--------------------------------------------------------------
SET
ESQUEMAHIVE = ${hivevar:esquema};

SET
Fecha_Proceso = ${hivevar:FechaProceso};

SET
MesTres = ${hivevar:mesTres};

SET
MesVeinteCuatro = ${hivevar:mesVeinteCuatro};

SET
MesVeinteCuatroDate = ${hivevar:mesVeinteCuatroDate};

SET
MesTresDate = ${hivevar:mesTresDate};

--n01
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_parque;

CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_parque AS
  SELECT
	date_format(last_day(add_months(current_date,
	-1)),
	'yyyyMMdd')
UNION 
  SELECT
	date_format(last_day(add_months(current_date,
	-2)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-3)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-4)),
	'yyyyMMdd')
UNION 
  SELECT
	date_format(last_day(add_months(current_date,
	-5)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-6)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-7)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-8)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-9)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-10)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-11)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-12)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-13)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-14)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-15)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-16)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-17)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-18)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-19)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-20)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-21)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-22)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-23)),
	'yyyyMMdd')
UNION
  SELECT
	date_format(last_day(add_months(current_date,
	-24)),
	'yyyyMMdd');

--n02
--ULTIMO  PARQUE 360
truncate TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion;

INSERT
	INTO
	${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion PARTITION (fecha_proceso)
SELECT
	num_telefonico,
	identificacion_cliente,
	segmento,
	sub_segmento,
	account_num,
	ciclo_facturacion,
	nombre_cliente,
	fecha_alta,
	nombre_plan,
	codigo_plan,
	tarifa,
	fecha_proceso
FROM
	db_reportes.otc_t_360_general
WHERE
	segmento IN ('NEGOCIOS', 'GGCC')
	AND CATEGORIA_PLAN = 'VOZ'
	AND es_parque = 'SI'
	AND upper(linea_negocio_homologado)= 'POSPAGO'
	AND fecha_proceso IN (
	SELECT
		*
	FROM
		${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_parque);
--N03
-----------------TERMINALES 24 MESES 
truncate TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_terminales_adendum;
--n01
INSERT
	INTO
	${hiveconf:ESQUEMAHIVE}.otc_t_b2b_terminales_adendum PARTITION(fecha_proceso = ${hiveconf:Fecha_Proceso})
SELECT
	*
FROM
	(
	SELECT
		CASE
			WHEN M.des_brand IS NULL THEN 'NO DEFINIDO'
			ELSE M.des_brand
		END marca,
		fabricante,
		fecha_factura,
		num_factura,
		monto monto_terminal,
		costo_total costo_terminal,
		monto-costo_total subsidio_terminal,
		telefono,
		T.account_num,
		A.PENALTYAMOUNT,
		ROW_NUMBER() OVER (PARTITION BY telefono
	ORDER BY
		fecha_factura DESC ) AS rank_alias
	FROM
		db_cs_terminales.otc_t_terminales_simcards T
	INNER JOIN (
		SELECT
			DISTINCT identificacion_cliente
		FROM
			${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion) PF ON
		PF.identificacion_cliente = T.identificacion_cliente
	LEFT JOIN db_urm.d_tacs M ON
		M.tac = substring(T.imei, 1, 8)
	LEFT JOIN db_rdb.otc_t_adendum A ON
		A.PHONE_NUMBER = T.telefono
			AND A.PHONE_NUMBER IS NOT NULL
			AND T.telefono IS NOT NULL
		WHERE
			clasificacion = 'TERMINALES'
			AND tipo_cargo = 'CARGO'
			AND p_fecha_factura>${hiveconf:MesVeinteCuatro} ) a
WHERE
	a.rank_alias = 1;

INSERT
	INTO
	${hiveconf:ESQUEMAHIVE}.otc_t_b2b_terminales_adendum PARTITION(fecha_proceso = ${hiveconf:Fecha_Proceso})
SELECT
	CASE
		WHEN M.des_brand IS NULL THEN 'NO DEFINIDO'
		ELSE M.des_brand
	END marca,
	fabricante,
	fecha_factura,
	num_factura,
	monto monto_terminal,
	costo_total costo_terminal,
	monto-costo_total subsidio_terminal,
	telefono,
	T.account_num,
	A.PENALTYAMOUNT,
	ROW_NUMBER() OVER (PARTITION BY telefono
ORDER BY
	fecha_factura DESC ) AS rank_alias
FROM
	db_cs_terminales.otc_t_terminales_simcards T
INNER JOIN (
	SELECT
		DISTINCT identificacion_cliente
	FROM
		${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion) PF ON
	PF.identificacion_cliente = T.identificacion_cliente
LEFT JOIN db_urm.d_tacs M ON
	M.tac = substring(T.imei, 1, 8)
LEFT JOIN db_rdb.otc_t_adendum A ON
	A.PHONE_NUMBER = T.telefono
	AND A.PHONE_NUMBER IS NOT NULL
	AND T.telefono IS NOT NULL
WHERE
	clasificacion = 'TERMINALES'
	AND tipo_cargo = 'CARGO'
	AND T.telefono IS NULL
	AND p_fecha_factura>${hiveconf:MesVeinteCuatro};
--------------TABLAS TEMPORALES---------------------
----LIMPIAR TABLAS TEMPORALES
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito;

DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_factura_afectada;

DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_ajustes_nota_credito;

DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_disputa;
--N04
--EXTRAER NOTAS DE CREDITO BILL SEG
CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito AS
SELECT
	A.account_num AS cuenta_facturacion,
	A.bill_seq AS bill_seg_nota_credito,
	A.invoice_num AS num_nota_credito,
	A.bill_dtm fecha_nota_credito,
	B.origin_invoice_num num_factura_afectada
FROM
	db_rbm.otc_t_billsummary A,
	db_rbm.otc_t_tfnecu_billsummary B
WHERE
	A.account_num = B.account_num
	AND A.bill_seq = B.bill_seq
	AND A.bill_version = B.bill_version
	AND A.bill_status IN (1, 28)
	AND A.bill_dtm >'${hiveconf:MesVeinteCuatroDate}';

--N05
--EXTRAER LA FACTURA AFECTADA CON LA NOTA DE CREDITO
CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_factura_afectada AS
SELECT
	A.account_num AS cuenta_facturacion,
	A.bill_seq AS bill_seq_factura,
	B.num_factura_afectada AS num_factura_afectada
FROM
	db_rbm.otc_t_billsummary A
INNER JOIN ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito B ON
	B.num_factura_afectada = A.invoice_num
	AND A.account_num = B.cuenta_facturacion
WHERE
	A.bill_status IN (1, 28)
GROUP BY
	A.account_num,
	A.bill_seq,
	B.num_factura_afectada;

--N06 
--EXTRAER AJUSTES EFECTUADOS A LA NOTA DE CREDITO
CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_ajustes_nota_credito AS
SELECT
	A.*,
	C.dispute_seq
FROM
	db_rbm.otc_t_ADJUSTMENT C
INNER JOIN ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito A ON
	C.BILL_SEQ = A.bill_seg_nota_credito
	AND C.ACCOUNT_NUM = A.cuenta_facturacion
WHERE
	C.adjustment_dat>'${hiveconf:MesVeinteCuatroDate}';

--N07 
--EXTRAE DISPUTA DE LA FACTURA AFECTADA POR LA NOTA DE CREDITO

CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_disputa AS
SELECT
	F.*,
	E.DISPUTE_TYPE_NAME
FROM
	db_rbm.otc_t_dispute D
INNER JOIN db_rbm.otc_t_disputetype E ON
	D.DISPUTE_TYPE_ID = E.DISPUTE_TYPE_ID
INNER JOIN ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_factura_afectada G ON
	D.BILL_SEQ = G.bill_seq_factura
	AND D.ACCOUNT_NUM = G.cuenta_facturacion
INNER JOIN ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_ajustes_nota_credito F ON
	G.cuenta_facturacion = F.cuenta_facturacion
	AND D.DISPUTE_SEQ = F.dispute_seq;

--N08
----------------------------------------------------
---Extraer facturaciOn de tres meses posteriores incluyendo mes actual y alimentar tabla de fecturacion otc_t_b2b_facturacion

truncate TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_facturacion;

INSERT
	INTO
	${hiveconf:ESQUEMAHIVE}.otc_t_b2b_facturacion PARTITION(fecha_proceso = ${hiveconf:Fecha_Proceso})
SELECT
	CF.identificacion identificacion_cliente,
	CF.cuenta_facturacion,
	CF.telefono,
	CF.numero_factura,
	'FACTURA' tipo_documento,
	to_date(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CF.fecha_factura AS String),
	'yyyyMMdd'))) fecha_transaccion,
	'' disputa,
	'' usuario,
	sum(CF.valor_facturado)monto
FROM
	db_cs_facturacion.otc_t_c_semantica_fact CF
INNER JOIN (
	SELECT
		DISTINCT(account_num)cuentas
	FROM
		${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion) C ON
	C.cuentas = CF.cuenta_facturacion
WHERE
	fecha_proceso >= ${hiveconf:MesVeinteCuatro}
	AND fecha_proceso <= ${hiveconf:Fecha_Proceso}
	AND CF.codigo_tipo_factura = 2
	AND CF.descripcion_tipo_concepto_facturable = 'CARGOS'
GROUP BY
	CF.identificacion,
	CF.cuenta_facturacion,
	CF.telefono,
	CF.numero_factura,
	CF.fecha_factura
UNION ALL
SELECT
	CF.identificacion identificacion_cliente,
	CF.cuenta_facturacion,
	CF.telefono,
	CF.numero_factura,
	'NOTA DE CREDITO' tipo_documento,
	to_date(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CF.fecha_factura AS String),
	'yyyyMMdd'))) fecha_transaccion,
	D.dispute_type_name disputa,
	'' usuario,
	sum(CF.valor_facturado)monto
FROM
	db_cs_facturacion.otc_t_c_semantica_fact CF
	--inner join db_rbm.OTC_T_CASCA_FAC_RELACIONADA X on X.nota_credito=CF.numero_factura and  x.tipo_factura_relacionada='Factura Ciclo'
INNER JOIN (
	SELECT
		DISTINCT(account_num)cuentas
	FROM
		${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion) C ON
	C.cuentas = CF.cuenta_facturacion
LEFT JOIN (
	SELECT
		DISTINCT cuenta_facturacion,
		bill_seg_nota_credito,
		num_nota_credito,
		fecha_nota_credito,
		num_factura_afectada,
		dispute_type_name
	FROM
		${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_disputa ) D ON
	D.cuenta_facturacion = CF.cuenta_facturacion
	AND D.num_nota_credito = CF.numero_factura
WHERE
	CF.fecha_proceso>${hiveconf:MesVeinteCuatro}
	AND CF.fecha_proceso <= ${hiveconf:Fecha_Proceso}
	AND CF.codigo_tipo_factura = 25
	AND CF.descripcion_tipo_concepto_facturable = 'CARGOS'
	--filtros disputas universo 
	AND (D.dispute_type_name LIKE 'NEGOCIACIÃ“N EJECUTIVO%'
		OR D.dispute_type_name LIKE 'LEALTAD Y RETENCIÃ“N%')
GROUP BY
	CF.identificacion,
	CF.cuenta_facturacion,
	CF.telefono,
	CF.numero_factura,
	CF.fecha_factura,
	D.dispute_type_name;
----LIMPIAR TEMPORALES
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito;

DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_factura_afectada;

DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_ajustes_nota_credito;

DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_disputa;

DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_parque;
