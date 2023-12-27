# -- coding: utf-8 --
import sys
reload(sys)
from query import *
from pyspark.sql import SparkSession, DataFrame
from pyspark_llap import HiveWarehouseSession
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
hive_hwc = HiveWarehouseSession.session(spark).build()
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))

##STEP 4:QUERYS
print(lne_dvs())
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
timestart_b = datetime.now()

vStp='[Paso 01]:Ejecucion de funcion [otc_t_b2b_temp_parque] '
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=otc_t_b2b_temp_parque()
    print(etq_sql(VSQL))
    df01 = spark.sql(VSQL).cache()
    ts_step_count = datetime.now()
    vTotDf=df01.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df01',vle_duracion(ts_step_count,te_step_count))))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df01')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive('otc_t_b2b_temp_parque')))
            df01.createOrReplaceTempView("otc_t_b2b_temp_parque")
            #df01.repartition(1).write.mode('overwrite').saveAsTable('otc_t_b2b_temp_parque')
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive('otc_t_b2b_temp_parque',str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive('otc_t_b2b_temp_parque',vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive('otc_t_b2b_temp_parque',str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df01')))
    del df01
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
print(lne_dvs())

vStp='[Paso 02]:Ejecucion de funcion [insert_otc_t_b2b_parque_facturacion] - ULTIMO  PARQUE 360'
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("REALIZA EL TRUNCADO DE LA TABLA: "+vSchmRep+"."+"otc_t_b2b_parque_facturacion"))
    query_truncate = "TRUNCATE TABLE "+vSchmRep+"."+"otc_t_b2b_parque_facturacion"
    print(query_truncate)
    spark.sql(query_truncate)
    VSQL=insert_otc_t_b2b_parque_facturacion()
    print(etq_sql(VSQL))
    df02 = spark.sql(VSQL).cache()
    ts_step_count = datetime.now()
    vTotDf=df02.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df02',vle_duracion(ts_step_count,te_step_count))))
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df02')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive('insert_otc_t_b2b_parque_facturacion')))
            columns = spark.table(vSchmRep+"."+"otc_t_b2b_parque_facturacion").columns
            cols = []
            for column in columns:
                cols.append(column)
            df02 = df02.select(cols)
            df02.repartition(1).write.mode("append").insertInto(vSchmRep+"."+"otc_t_b2b_parque_facturacion")
            print(etq_info("Insercion Ok de la tabla destino: "+"otc_t_b2b_parque_facturacion")) 
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive('insert_otc_t_b2b_parque_facturacion',str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive('insert_otc_t_b2b_parque_facturacion',vle_duracion(ts_step_tbl,te_step_tbl))))
            spark.catalog.dropTempView("otc_t_b2b_temp_parque")
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive('insert_otc_t_b2b_parque_facturacion',str(e))))
    del df02
    print(etq_info("Eliminar dataframe [{}]".format('df02')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
print(lne_dvs())

vStp='[Paso 03]:Ejecucion de funcion [insert_01_otc_t_b2b_terminales_adendum] - TERMINALES 24 MESES'
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("REALIZA EL TRUNCADO DE LA TABLA: "+vSchmRep+"."+"otc_t_b2b_terminales_adendum"))
    query_truncate = "TRUNCATE TABLE "+vSchmRep+"."+"otc_t_b2b_terminales_adendum"
    print(query_truncate)
    spark.sql(query_truncate)
    VSQL_1=otc_t_b2b_identificacion_cliente(vSchmRep)
    df03_01 = spark.sql(VSQL_1).cache()
    df03_01.printSchema()
    df03_01.createOrReplaceTempView("otc_t_b2b_identificacion_cliente")
    VSQL=insert_01_otc_t_b2b_terminales_adendum(mesVeinteCuatro,FechaProceso)
    print(etq_sql(VSQL))
    df03 = spark.sql(VSQL).cache()
    ts_step_count = datetime.now()
    vTotDf=df03.count()
    print(etq_info(msg_t_total_registros_hive('insert_01_otc_t_b2b_terminales_adendum',str(vTotDf))))
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df03',vle_duracion(ts_step_count,te_step_count))))
    
    del df03_01
    print(etq_info("Eliminar dataframe [{}]".format('df03_01')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
print(lne_dvs())

vStp='[Paso 04]:Ejecucion de funcion [insert_02_otc_t_b2b_terminales_adendum] - TERMINALES 24 MESES'
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=insert_02_otc_t_b2b_terminales_adendum(mesVeinteCuatro,FechaProceso)
    print(etq_sql(VSQL))
    df04 = spark.sql(VSQL).cache()
    df04 = df04.union(df03)
    ts_step_count = datetime.now()
    vTotDf=df04.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df04',vle_duracion(ts_step_count,te_step_count))))
    if df04.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df04')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive('insert_02_otc_t_b2b_terminales_adendum')))
            columns = spark.table(vSchmRep+"."+"otc_t_b2b_terminales_adendum").columns
            cols = []
            for column in columns:
                cols.append(column)
            df04 = df04.select(cols)
            df04.repartition(1).write.mode("append").insertInto(vSchmRep+"."+"otc_t_b2b_terminales_adendum")
            print(etq_info("Insercion Ok de la tabla destino: "+"otc_t_b2b_terminales_adendum")) 
            df04.printSchema()
            print(etq_info(msg_t_total_registros_hive('insert_02_otc_t_b2b_terminales_adendum',str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive('insert_02_otc_t_b2b_terminales_adendum',vle_duracion(ts_step_tbl,te_step_tbl))))
            spark.catalog.dropTempView("otc_t_b2b_identificacion_cliente")
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive('insert_02_otc_t_b2b_terminales_adendum',str(e))))
    del df03
    del df04
    print(etq_info("Eliminar dataframe [{}]".format('df04')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
print(lne_dvs())

try:
    vStp="Paso [05]: Ejecucion de funcion [otc_t_b2b_temp_notas_credito]- EXTRAER NOTAS DE CREDITO BILL SEG"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    df05=spark.sql(otc_t_b2b_temp_notas_credito(mesVeinteCuatroDate)).cache()
    df05.printSchema()
    ts_step_tbl = datetime.now()
    df05.createOrReplaceTempView("otc_t_b2b_temp_notas_credito")
    print(etq_info(msg_t_total_registros_obtenidos("df05",str(df05.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df05",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df05
    
    vStp="Paso [06]: Ejecucion de funcion [otc_t_b2b_temp_factura_afectada]- EXTRAER LA FACTURA AFECTADA CON LA NOTA DE CREDITO"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    df06=spark.sql(otc_t_b2b_temp_factura_afectada()).cache()
    df06.printSchema()
    ts_step_tbl = datetime.now()
    df06.createOrReplaceTempView("otc_t_b2b_temp_factura_afectada")
    print(etq_info(msg_t_total_registros_obtenidos("df06",str(df06.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df06",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df06
    
    vStp="Paso [07]: Ejecucion de funcion [otc_t_b2b_temp_ajustes_nota_credito]- EXTRAER AJUSTES EFECTUADOS A LA NOTA DE CREDITO"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    df07=spark.sql(otc_t_b2b_temp_ajustes_nota_credito(mesVeinteCuatroDate)).cache()
    df07.printSchema()
    ts_step_tbl = datetime.now()
    df07.createOrReplaceTempView("otc_t_b2b_temp_ajustes_nota_credito")
    print(etq_info(msg_t_total_registros_obtenidos("df07",str(df07.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df07",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df07
    spark.catalog.dropTempView("otc_t_b2b_temp_notas_credito")
    
    vStp="Paso [08]: Ejecucion de funcion [otc_t_b2b_temp_disputa]- EXTRAE DISPUTA DE LA FACTURA AFECTADA POR LA NOTA DE CREDITO"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    df08=spark.sql(otc_t_b2b_temp_disputa()).cache()
    df08.printSchema()
    ts_step_tbl = datetime.now()
    df08.createOrReplaceTempView("otc_t_b2b_temp_disputa")
    print(etq_info(msg_t_total_registros_obtenidos("df08",str(df08.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df08",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df08
    spark.catalog.dropTempView("otc_t_b2b_temp_factura_afectada")
    spark.catalog.dropTempView("otc_t_b2b_temp_ajustes_nota_credito")
    
    vStp="Paso [09]: Ejecucion de funcion [otc_t_b2b_cuentas]- SE OBTIENE LOS DIFERENTES ACCOUNT NUMBERS"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    df09=spark.sql(otc_t_b2b_cuentas(vSchmRep)).cache()
    df09.printSchema()
    ts_step_tbl = datetime.now()
    df09.createOrReplaceTempView("otc_t_b2b_cuentas")
    print(etq_info(msg_t_total_registros_obtenidos("df09",str(df09.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df09",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df09
    
    vStp="Paso [10]: Ejecucion de funcion [otc_t_b2b_disp]- Extrae informacion de disputa"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    df10=spark.sql(otc_t_b2b_disp()).cache()
    df10.printSchema()
    ts_step_tbl = datetime.now()
    df10.createOrReplaceTempView("otc_t_b2b_disp")
    print(etq_info(msg_t_total_registros_obtenidos("df10",str(df10.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df10",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df10
    
    vStp="Paso [11]: Ejecucion de funcion [otc_t_b2b_facturacion_1]-Extraer facturacion de tres meses posteriores incluyendo mes actual y alimentar tabla de facturacion otc_t_b2b_facturacion"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    df11=spark.sql(otc_t_b2b_facturacion_1(mesVeinteCuatro,FechaProceso)).cache()
    df11.printSchema()
    ts_step_tbl = datetime.now()
    print(etq_info(msg_t_total_registros_obtenidos("df11",str(df11.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df11",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp="Paso [12]: Ejecucion de funcion [otc_t_b2b_facturacion_2]-Extraer facturacion de tres meses posteriores incluyendo mes actual y alimentar tabla de facturacion otc_t_b2b_facturacion"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    df12=spark.sql(otc_t_b2b_facturacion_2(mesVeinteCuatro,FechaProceso)).cache()
    df12.printSchema()
    ts_step_tbl = datetime.now()
    print(etq_info(msg_t_total_registros_obtenidos("df12",str(df12.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df12",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("otc_t_b2b_cuentas")
    spark.catalog.dropTempView("otc_t_b2b_disp")
    
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp,str(e))))

vStp='[Paso final]:UNION ALL e INSERT en tabla final de los dataframes obtenidos en los PASOS [11] y [12] - Extraer facturacion de tres meses posteriores incluyendo mes actual y alimentar tabla de facturacion otc_t_b2b_facturacion'
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("REALIZA EL TRUNCADO DE LA TABLA: "+vSchmRep+"."+"otc_t_b2b_facturacion"))
    query_truncate = "TRUNCATE TABLE "+vSchmRep+"."+"otc_t_b2b_facturacion"
    print(query_truncate)
    spark.sql(query_truncate)
    dffinal = df11.union(df12)
    ts_step_count = datetime.now()
    vTotDf=dffinal.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('dffinal',vle_duracion(ts_step_count,te_step_count))))
    if dffinal.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('dffinal')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive('otc_t_b2b_facturacion')))
            columns = spark.table(vSchmRep+"."+"otc_t_b2b_facturacion").columns
            cols = []
            for column in columns:
                cols.append(column)
            dffinal = dffinal.select(cols)
            dffinal.repartition(2).write.mode("append").insertInto(vSchmRep+"."+"otc_t_b2b_facturacion")
            print(etq_info("Insercion Ok de la tabla destino: "+"otc_t_b2b_facturacion")) 
            dffinal.printSchema()
            print(etq_info(msg_t_total_registros_hive('otc_t_b2b_facturacion',str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive('otc_t_b2b_facturacion',vle_duracion(ts_step_tbl,te_step_tbl))))
            #spark.catalog.dropTempView("otc_t_b2b_temp_disputa")
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive('otc_t_b2b_facturacion',str(e))))
    del dffinal
    del df11
    del df12
    print(etq_info("Eliminar dataframe [{}]".format('dffinal')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
    #print('Total de registros insertados en la tabla:'+vSchmRep+"."+"otc_t_b2b_facturacion"+'WHERE fecha_proceso='+FechaProceso)
    #print(spark.sql('Select count(1) from ' +vSchmRep+"."+"otc_t_b2b_facturacion"))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
print(lne_dvs())


spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion("CARGA_INFORMACION_B2B",vle_duracion(timestart,timeend))))
print(lne_dvs())
