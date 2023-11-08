# -- coding: utf-8 --
## N01
def otc_t_b2b_temp_parque():
    qry="""
    SELECT 
        date_format(last_day(add_months(current_date, -1)),'yyyyMMdd') as fecha_proceso
    UNION 
    SELECT 
        date_format(last_day(add_months(current_date, -2)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -3)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -4)),'yyyyMMdd') as fecha_proceso
    UNION 
    SELECT 
        date_format(last_day(add_months(current_date, -5)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -6)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -7)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -8)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -9)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -10)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -11)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -12)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -13)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -14)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -15)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -16)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -17)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -18)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -19)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -20)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -21)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -22)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -23)),'yyyyMMdd') as fecha_proceso
    UNION
    SELECT 
        date_format(last_day(add_months(current_date, -24)),'yyyyMMdd') as fecha_proceso
    """
    return qry

## N02 - ULTIMO  PARQUE 360
def insert_otc_t_b2b_parque_facturacion():
    qry="""
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
            fecha_proceso
        FROM
            otc_t_b2b_temp_parque)
    """
    return qry

## N03 --TERMINALES 24 MESES 
# trunca/carga 
def insert_01_otc_t_b2b_terminales_adendum(vSchmRep,mesVeinteCuatro,FechaProceso):
    qry="""
SELECT
	marca
	, fabricante AS modelo_terminal
	, fecha_factura
	, num_factura
	, monto_terminal
	, costo_terminal
	, subsidio_terminal
	, telefono
	, account_num
	, penaltyamount AS adendum
	, rank_alias AS rnk
	, {FechaProceso} as fecha_proceso
FROM
	(
	SELECT
		CASE
			WHEN M.des_brand IS NULL THEN 'NO DEFINIDO'
			ELSE M.des_brand END marca,
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
			{vSchmRep}.otc_t_b2b_parque_facturacion) PF ON
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
			AND p_fecha_factura>{mesVeinteCuatro} ) a
WHERE
	a.rank_alias = 1
    """.format(vSchmRep=vSchmRep,mesVeinteCuatro=mesVeinteCuatro,FechaProceso=FechaProceso)
    return qry

## N04
def insert_02_otc_t_b2b_terminales_adendum(vSchmRep,mesVeinteCuatro,FechaProceso):
    qry="""
SELECT
	CASE
		WHEN M.des_brand IS NULL THEN 'NO DEFINIDO'
		ELSE M.des_brand
	END marca,
	fabricante AS modelo_terminal,
	fecha_factura,
	num_factura,
	monto monto_terminal,
	costo_total costo_terminal,
	monto-costo_total subsidio_terminal,
	telefono,
	T.account_num,
	A.PENALTYAMOUNT AS adendum,
	ROW_NUMBER() OVER (PARTITION BY telefono
ORDER BY
	fecha_factura DESC ) AS rnk
	, {FechaProceso} as fecha_proceso
FROM
	db_cs_terminales.otc_t_terminales_simcards T
INNER JOIN (
	SELECT
		DISTINCT identificacion_cliente
	FROM
		{vSchmRep}.otc_t_b2b_parque_facturacion) PF ON
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
	AND p_fecha_factura>{mesVeinteCuatro}
    """.format(vSchmRep=vSchmRep,mesVeinteCuatro=mesVeinteCuatro,FechaProceso=FechaProceso)
    return qry

## N05 --EXTRAER NOTAS DE CREDITO BILL SEG
def otc_t_b2b_temp_notas_credito(mesVeinteCuatroDate):
    qry="""
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
	AND A.bill_dtm >'{mesVeinteCuatroDate}'
    """.format(mesVeinteCuatroDate=mesVeinteCuatroDate)
    print(qry)
    return qry  

## N06 --EXTRAER LA FACTURA AFECTADA CON LA NOTA DE CREDITO
def otc_t_b2b_temp_factura_afectada():
    qry="""
SELECT
	A.account_num AS cuenta_facturacion,
	A.bill_seq AS bill_seq_factura,
	B.num_factura_afectada AS num_factura_afectada
FROM
	db_rbm.otc_t_billsummary A
INNER JOIN otc_t_b2b_temp_notas_credito B ON
	B.num_factura_afectada = A.invoice_num
	AND A.account_num = B.cuenta_facturacion
WHERE
	A.bill_status IN (1, 28)
GROUP BY
	A.account_num,
	A.bill_seq,
	B.num_factura_afectada
    """
    print(qry)
    return qry  

## N07 --EXTRAER AJUSTES EFECTUADOS A LA NOTA DE CREDITO
def otc_t_b2b_temp_ajustes_nota_credito(mesVeinteCuatroDate):
    qry="""
SELECT
	a.cuenta_facturacion
	, a.bill_seg_nota_credito
	, a.num_nota_credito
	, a.fecha_nota_credito
	, a.num_factura_afectada
	, C.dispute_seq
FROM
	db_rbm.otc_t_ADJUSTMENT C
INNER JOIN otc_t_b2b_temp_notas_credito A ON
	C.BILL_SEQ = A.bill_seg_nota_credito
	AND C.ACCOUNT_NUM = A.cuenta_facturacion
WHERE
	C.adjustment_dat>'{mesVeinteCuatroDate}'
    """.format(mesVeinteCuatroDate=mesVeinteCuatroDate)
    print(qry)
    return qry  

## N08 --EXTRAE DISPUTA DE LA FACTURA AFECTADA POR LA NOTA DE CREDITO
def otc_t_b2b_temp_disputa():
    qry="""
SELECT
	f.cuenta_facturacion
	, f.bill_seg_nota_credito
	, f.num_nota_credito
	, f.fecha_nota_credito
	, f.num_factura_afectada
	, f.dispute_seq
	, E.DISPUTE_TYPE_NAME
FROM
	db_rbm.otc_t_dispute D
INNER JOIN db_rbm.otc_t_disputetype E ON
	D.DISPUTE_TYPE_ID = E.DISPUTE_TYPE_ID
INNER JOIN otc_t_b2b_temp_factura_afectada G ON
	D.BILL_SEQ = G.bill_seq_factura
	AND D.ACCOUNT_NUM = G.cuenta_facturacion
INNER JOIN otc_t_b2b_temp_ajustes_nota_credito F ON
	G.cuenta_facturacion = F.cuenta_facturacion
	AND D.DISPUTE_SEQ = F.dispute_seq
    """
    print(qry)
    return qry  

## N09 --SE OBTIENE LOS DIFERENTES ACCOUNT NUMBERS
def otc_t_b2b_cuentas(vSchmRep):
    qry="""
SELECT
	DISTINCT(account_num)cuentas
FROM
	{vSchmRep}.otc_t_b2b_parque_facturacion
    """.format(vSchmRep=vSchmRep)
    return qry  

## N10 --Extrae informacion de disputa
def otc_t_b2b_disp():
    qry="""
SELECT
	DISTINCT 
	cuenta_facturacion,
	bill_seg_nota_credito,
	num_nota_credito,
	fecha_nota_credito,
	num_factura_afectada,
	dispute_type_name
FROM
	otc_t_b2b_temp_disputa 
    """
    return qry  

## N11 --Extraer facturacion de tres meses posteriores incluyendo mes actual y alimentar tabla de facturacion otc_t_b2b_facturacion
def otc_t_b2b_facturacion_1(mesVeinteCuatro,FechaProceso):
    qry="""
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
	sum(CF.valor_facturado)monto,
	{FechaProceso} AS fecha_proceso
FROM
	db_cs_facturacion.otc_t_c_semantica_fact CF
INNER JOIN otc_t_b2b_cuentas C ON
	C.cuentas = CF.cuenta_facturacion
WHERE
	fecha_proceso >= {mesVeinteCuatro}
	AND fecha_proceso <= {FechaProceso}
	AND CF.codigo_tipo_factura = 2
	AND CF.descripcion_tipo_concepto_facturable = 'CARGOS'
GROUP BY
	CF.identificacion,
	CF.cuenta_facturacion,
	CF.telefono,
	CF.numero_factura,
	CF.fecha_factura
    """.format(mesVeinteCuatro=mesVeinteCuatro,FechaProceso=FechaProceso)
    return qry  

## N12 --Extraer facturacion de tres meses posteriores incluyendo mes actual y alimentar tabla de facturacion otc_t_b2b_facturacion
def otc_t_b2b_facturacion_2(mesVeinteCuatro,FechaProceso):
    qry="""
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
	sum(CF.valor_facturado)monto,
	{FechaProceso} AS fecha_proceso
FROM
	db_cs_facturacion.otc_t_c_semantica_fact CF
--inner join db_rbm.OTC_T_CASCA_FAC_RELACIONADA X on X.nota_credito=CF.numero_factura and  x.tipo_factura_relacionada='Factura Ciclo'
INNER JOIN otc_t_b2b_cuentas C ON
	C.cuentas = CF.cuenta_facturacion
LEFT JOIN otc_t_b2b_disp D ON
	D.cuenta_facturacion = CF.cuenta_facturacion
	AND D.num_nota_credito = CF.numero_factura
WHERE
	CF.fecha_proceso>{mesVeinteCuatro}
	AND CF.fecha_proceso <= {FechaProceso}
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
	D.dispute_type_name
    """.format(mesVeinteCuatro=mesVeinteCuatro,FechaProceso=FechaProceso)
    return qry  

