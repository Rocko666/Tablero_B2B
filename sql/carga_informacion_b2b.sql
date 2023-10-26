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
set ESQUEMAHIVE=${hivevar:esquema};
set Fecha_Proceso=${hivevar:FechaProceso};
set MesTres=${hivevar:mesTres};
set MesVeinteCuatro=${hivevar:mesVeinteCuatro};
set MesVeinteCuatroDate=${hivevar:mesVeinteCuatroDate};
set MesTresDate=${hivevar:mesTresDate};

drop table ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_parque;
create table ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_parque as
  Select 
  date_format(last_day(add_months(current_date, -1)),'yyyyMMdd')
  union 
  Select 
  date_format(last_day(add_months(current_date, -2)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -3)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -4)),'yyyyMMdd')
  union 
  Select 
  date_format(last_day(add_months(current_date, -5)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -6)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -7)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -8)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -9)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -10)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -11)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -12)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -13)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -14)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -15)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -16)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -17)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -18)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -19)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -20)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -21)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -22)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -23)),'yyyyMMdd')
  union
  Select 
  date_format(last_day(add_months(current_date, -24)),'yyyyMMdd');

--ULTIMO  PARQUE 360
truncate table ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion;
insert into ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion partition (fecha_proceso)
select num_telefonico,
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
from db_reportes.otc_t_360_general
where segmento in ('NEGOCIOS','GGCC') and CATEGORIA_PLAN='VOZ' and es_parque='SI' and upper(linea_negocio_homologado)='POSPAGO' and fecha_proceso in (select * from ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_parque);
  
-----------------TERMINALES 24 MESES 
truncate table ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_terminales_adendum;
insert into ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_terminales_adendum partition(fecha_proceso=${hiveconf:Fecha_Proceso})
 select * from (
 select
  CASE WHEN M.des_brand IS NULL THEN 'NO DEFINIDO' ELSE M.des_brand END marca,
fabricante,
fecha_factura,
num_factura,
monto monto_terminal,
costo_total costo_terminal,
monto-costo_total  subsidio_terminal,
telefono,
T.account_num,
A.PENALTYAMOUNT,
   row_number() over (partition by telefono order by fecha_factura desc ) as rank_alias
 from db_cs_terminales.otc_t_terminales_simcards T
inner join (select distinct identificacion_cliente from ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion) PF on PF.identificacion_cliente=T.identificacion_cliente
left join  db_urm.d_tacs M on M.tac=substring(T.imei,1,8)
left join  db_rdb.otc_t_adendum  A on A.PHONE_NUMBER=T.telefono and A.PHONE_NUMBER is not null and T.telefono is not null
where clasificacion='TERMINALES' and  tipo_cargo='CARGO' and p_fecha_factura>${hiveconf:MesVeinteCuatro} ) a where a.rank_alias = 1;

insert into ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_terminales_adendum partition(fecha_proceso=${hiveconf:Fecha_Proceso})
select
  CASE WHEN M.des_brand IS NULL THEN 'NO DEFINIDO' ELSE M.des_brand END marca,
fabricante,
fecha_factura,
num_factura,
monto monto_terminal,
costo_total costo_terminal,
monto-costo_total  subsidio_terminal,
telefono,
T.account_num,
A.PENALTYAMOUNT,
   row_number() over (partition by telefono order by fecha_factura desc ) as rank_alias
 from db_cs_terminales.otc_t_terminales_simcards T
inner join (select distinct identificacion_cliente from ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion) PF on PF.identificacion_cliente=T.identificacion_cliente
left join  db_urm.d_tacs M on M.tac=substring(T.imei,1,8)
left join  db_rdb.otc_t_adendum  A on A.PHONE_NUMBER=T.telefono  and A.PHONE_NUMBER is not null and T.telefono is not null
where clasificacion='TERMINALES' and  tipo_cargo='CARGO'  and T.telefono is null and p_fecha_factura>${hiveconf:MesVeinteCuatro};
--------------TABLAS TEMPORALES---------------------
----LIMPIAR TABLAS TEMPORALES
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito;
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_factura_afectada;
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_ajustes_nota_credito;
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_disputa;

--EXTRAER NOTAS DE CREDITO BILL SEG
CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito as
SELECT A.account_num as cuenta_facturacion,
A.bill_seq  as bill_seg_nota_credito,
A.invoice_num as num_nota_credito,
A.bill_dtm fecha_nota_credito,
B.origin_invoice_num num_factura_afectada
FROM  db_rbm.otc_t_billsummary A, 
db_rbm.otc_t_tfnecu_billsummary B
WHERE A.account_num=B.account_num
AND A.bill_seq = B.bill_seq
AND A.bill_version = B.bill_version
AND A.bill_status IN (1,28)
AND A.bill_dtm >'${hiveconf:MesVeinteCuatroDate}'; 

--EXTRAER LA FACTURA AFECTADA CON LA NOTA DE CREDITO
CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_factura_afectada AS
SELECT A.account_num AS cuenta_facturacion,
A.bill_seq AS bill_seq_factura,
B.num_factura_afectada as num_factura_afectada
FROM db_rbm.otc_t_billsummary A 
INNER JOIN ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito B ON B.num_factura_afectada=A.invoice_num 
and A.account_num=B.cuenta_facturacion
WHERE A.bill_status IN (1,28)
group by A.account_num,
A.bill_seq,
B.num_factura_afectada;


--EXTRAER AJUSTES EFECTUADOS A LA NOTA DE CREDITO
CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_ajustes_nota_credito AS
select A.*,C.dispute_seq from db_rbm.otc_t_ADJUSTMENT C
INNER JOIN ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito A on C.BILL_SEQ=A.bill_seg_nota_credito and C.ACCOUNT_NUM=A.cuenta_facturacion
where C.adjustment_dat>'${hiveconf:MesVeinteCuatroDate}';

--EXTRAE DISPUTA DE LA FACTURA AFECTADA POR LA NOTA DE CREDITO

CREATE TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_disputa as
SELECT 
F.*,E.DISPUTE_TYPE_NAME
FROM db_rbm.otc_t_dispute D
INNER JOIN  db_rbm.otc_t_disputetype E ON  D.DISPUTE_TYPE_ID = E.DISPUTE_TYPE_ID		
INNER JOIN  ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_factura_afectada G on D.BILL_SEQ=G.bill_seq_factura and D.ACCOUNT_NUM=G.cuenta_facturacion
INNER JOIN  ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_ajustes_nota_credito F on G.cuenta_facturacion=F.cuenta_facturacion and D.DISPUTE_SEQ=F.dispute_seq;
----------------------------------------------------

---Extraer facturaciOn de tres meses posteriores incluyendo mes actual y alimentar tabla de fecturacion otc_t_b2b_facturacion

truncate table ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_facturacion;
insert into ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_facturacion partition(fecha_proceso=${hiveconf:Fecha_Proceso})
select
CF.identificacion identificacion_cliente,
CF.cuenta_facturacion,
CF.telefono,
CF.numero_factura,
'FACTURA' tipo_documento,
to_date(FROM_UNIXTIME(UNIX_TIMESTAMP(cast(CF.fecha_factura as String),'yyyyMMdd'))) fecha_transaccion,
'' disputa,
'' usuario,
sum(CF.valor_facturado)monto
from db_cs_facturacion.otc_t_c_semantica_fact CF 
inner join (select distinct(account_num)cuentas  from ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion) C on C.cuentas=CF.cuenta_facturacion
where fecha_proceso>=${hiveconf:MesVeinteCuatro} and fecha_proceso<=${hiveconf:Fecha_Proceso}
and CF.codigo_tipo_factura=2
and CF.descripcion_tipo_concepto_facturable='CARGOS'
group by CF.identificacion,CF.cuenta_facturacion,CF.telefono,CF.numero_factura,CF.fecha_factura
union all
select
CF.identificacion identificacion_cliente,
CF.cuenta_facturacion,
CF.telefono,
CF.numero_factura,
'NOTA DE CREDITO' tipo_documento,
to_date(FROM_UNIXTIME(UNIX_TIMESTAMP(cast(CF.fecha_factura as String),'yyyyMMdd'))) fecha_transaccion,
D.dispute_type_name disputa,
'' usuario,
sum(CF.valor_facturado)monto
from db_cs_facturacion.otc_t_c_semantica_fact CF   
--inner join db_rbm.OTC_T_CASCA_FAC_RELACIONADA X on X.nota_credito=CF.numero_factura and  x.tipo_factura_relacionada='Factura Ciclo'
inner join (select distinct(account_num)cuentas  from ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_parque_facturacion) C on C.cuentas=CF.cuenta_facturacion
left join (select distinct cuenta_facturacion,
bill_seg_nota_credito,
num_nota_credito,
fecha_nota_credito,
num_factura_afectada,
dispute_type_name from  ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_disputa ) D on D.cuenta_facturacion=CF.cuenta_facturacion and D.num_nota_credito=CF.numero_factura
where CF.fecha_proceso>${hiveconf:MesVeinteCuatro} and CF.fecha_proceso<=${hiveconf:Fecha_Proceso}
and CF.codigo_tipo_factura=25
and CF.descripcion_tipo_concepto_facturable='CARGOS'
--filtros disputas universo 
and (D.dispute_type_name like 'NEGOCIACIÃ“N EJECUTIVO%' or D.dispute_type_name like 'LEALTAD Y RETENCIÃ“N%')
group by CF.identificacion,CF.cuenta_facturacion,CF.telefono,CF.numero_factura,CF.fecha_factura,D.dispute_type_name;

----LIMPIAR TEMPORALES
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_notas_credito;
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_factura_afectada;
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_ajustes_nota_credito;
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_disputa;
DROP TABLE ${hiveconf:ESQUEMAHIVE}.otc_t_b2b_temp_parque;
