# -- coding: utf-8 --
import sys
reload(sys)
from query import *
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql.functions import *
import argparse
from datetime import datetime, timedelta
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

## STEP 1: Definir variables o constantes
vLogInfo='INFO:'
vLogError='ERROR:'

timestart = datetime.now()
## STEP 2: Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser.add_argument('--vSchmRep', required=True, type=str,help='Esquema HIVE reportes')
parser.add_argument('--FechaProceso', required=True, type=str,help='Fecha final mes anterior')
parser.add_argument('--mesTres', required=True, type=str,help='FECHA EJECUCION MENOS 3 MESES')
parser.add_argument('--mesTresDate', required=True, type=str,help='FECHA EJECUCION MENOS 3 MESES formato date')
parser.add_argument('--mesVeinteCuatro', required=True, type=str,help='FECHA EJECUCION MENOS 24 MESES ')
parser.add_argument('--mesVeinteCuatroDate', required=True, type=str,help='FECHA EJECUCION MENOS 24 MESES formato date ')

parametros = parser.parse_args()
vSchmRep=parametros.vSchmRep
FechaProceso=parametros.FechaProceso
mesTres=parametros.mesTres
mesTresDate=parametros.mesTresDate
mesVeinteCuatro=parametros.mesVeinteCuatro
mesVeinteCuatroDate=parametros.mesVeinteCuatroDate

## STEP 3: Inicio el SparkSession
spark = SparkSession. \
    builder. \
    config("hive.exec.dynamic.partition.mode", "nonstrict"). \
    enableHiveSupport(). \
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
app_id = spark._sc.applicationId
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))

##STEP 4:QUERYS
print(lne_dvs())
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
timestart_b = datetime.now()
try:
    vStp01="Paso 1"
    print(lne_dvs())
    print(etq_info("Paso [1]: Ejecucion de funcion [tmp_otc_t_ip_uni]- -CREA LA TABLA TEMPORAL DE IP CON REGISTROS UNICOS"))
    print(lne_dvs())
    df_tmp_otc_t_ip_uni=spark.sql(tmp_otc_t_ip_uni(vSchTmp)).cache()
    df_tmp_otc_t_ip_uni.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_ip_uni.createOrReplaceTempView("tmp_otc_t_ip_uni")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_ip_uni",str(df_tmp_otc_t_ip_uni.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_ip_uni",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 2"
    print(lne_dvs())
    print(etq_info("Paso [2]: Ejecucion de funcion [tmp_otc_t_iot_m2m]- CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M"))
    print(lne_dvs())
    df_tmp_otc_t_iot_m2m=spark.sql(tmp_otc_t_iot_m2m(vfecha_proceso)).cache()
    df_tmp_otc_t_iot_m2m.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_iot_m2m.createOrReplaceTempView("tmp_otc_t_iot_m2m")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_iot_m2m",str(df_tmp_otc_t_iot_m2m.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_iot_m2m",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 3"
    print(lne_dvs())
    print(etq_info("Paso [3]: Ejecucion de funcion [tmp_otc_t_iot_m2m_trafico_apn]-CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M"))
    print(lne_dvs())
    df_tmp_otc_t_iot_m2m_trafico_apn=spark.sql(tmp_otc_t_iot_m2m_trafico_apn(vfecha_proceso)).cache()
    df_tmp_otc_t_iot_m2m_trafico_apn.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_iot_m2m_trafico_apn.createOrReplaceTempView("tmp_otc_t_iot_m2m_trafico_apn")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_iot_m2m_trafico_apn",str(df_tmp_otc_t_iot_m2m_trafico_apn.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_iot_m2m_trafico_apn",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 4"
    print(lne_dvs())
    print(etq_info("Paso [4]: Ejecucion de funcion [tmp_otc_t_iot_m2m_trafico_apn_sin_dup]- CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M SIN DUPLICADOS"))
    print(lne_dvs())
    df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup=spark.sql(tmp_otc_t_iot_m2m_trafico_apn_sin_dup(vfecha_proceso)).cache()
    df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup.createOrReplaceTempView("tmp_otc_t_iot_m2m_trafico_apn_sin_dup")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup",str(df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 5"
    print(lne_dvs())
    print(etq_info("Paso [5]: Ejecucion de funcion [otc_t_iot_m2m_trafico_apn] - INSERTA LOS REGISTROS EN LA TABLA OTC_T_IOT_M2M_TRAFICO_APN)"))
    print(lne_dvs())
    print(etq_info("REALIZA EL BORRADO DE PARTICIONES CORRESPONDIENTES AL MES DE PROCESO (DESDE EL DIA 1 HASTA DIA CAIDO)"))
    query_truncate = "ALTER TABLE "+vSchRep+"."+vTFinal+" DROP IF EXISTS PARTITION (fecha_proceso = " +vfecha_proceso+ " ) purge"
    print(query_truncate)
    spark.sql(query_truncate)
    
    df_otc_t_iot_m2m_trafico_apn=spark.sql(otc_t_iot_m2m_trafico_apn(vfecha_proceso)).cache()
    df_otc_t_iot_m2m_trafico_apn.printSchema()
    ts_step_tbl = datetime.now()
    columns = spark.table(vSchRep+"."+vTFinal).columns
    cols = []
    for column in columns:
        cols.append(column)
    df_otc_t_iot_m2m_trafico_apn = df_otc_t_iot_m2m_trafico_apn.select(cols)
    df_otc_t_iot_m2m_trafico_apn.write.mode("append").insertInto(vSchRep+"."+vTFinal)
    print(etq_info("Insercion Ok de la tabla destino: "+str(vTFinal))) 
    print(etq_info(msg_t_total_registros_hive("df_otc_t_iot_m2m_trafico_apn",str(df_otc_t_iot_m2m_trafico_apn.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_otc_t_iot_m2m_trafico_apn",vle_duracion(ts_step_tbl,te_step_tbl))))
    
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()
    del df_otc_t_iot_m2m_trafico_apn
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(msg_e_ejecucion(vStpFin,str(e)))

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion("OTC_T_IOT_M2M_TRAFICO_APN",vle_duracion(timestart,timeend))))
print(lne_dvs())
