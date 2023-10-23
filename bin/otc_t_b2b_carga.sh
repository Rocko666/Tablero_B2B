#!/bin/bash
#############################################################################
# Proyecto:          Tablas finales tablero B2B                				#
# Cliente:           Telefonica                                 			#
# Elaborado por:     Softconsulting S.A.                        			#
# Script:            otc_t_b2b_carga.sh                         			#
# Fecha de creacion: 2020/07/18                                 			#
# Fecha de modificacion:                                        			#
# Fecha de modificacion:  2023/03/29                            			#
# User modificacion: Klever Amari                               			#
# Descripcion de modificacion: Se actualiza la sentencia hive por beeline   #
#############################################################################
set -e

FECHAEJE=$1 # Formato yyyyMMdd

ENTIDAD=OTC_T_B2B_PARQUE_FACTURACION
AMBIENTE=1 # AMBIENTE (1=produccion, 0=desarrollo)
ini_fecha=$(date '+%Y%m%d%H%M%S')
eval year=$(echo $FECHAEJE | cut -c1-4)
eval month=$(echo $FECHAEJE | cut -c5-6)
day="01"
fechaIniMes=$year$month$day #Formato YYYYMMDD
fechaIniMesDate=$(date '+%Y%m%d' -d "$fechaIniMes")
fechaFinMesAnterior=$(date -d "$fechaIniMesDate-1 day" +%Y%m%d)
fecha_eje1=$(date '+%Y%m%d' -d "$fechaFinMesAnterior")
MesTresDate=$(date -d "$fechaIniMes-3 month" +%Y-%m-%d)
MesVeinteCuatro=$(date -d "$fechaFinMesAnterior-24 month" +%Y%m%d)
MesVeinteCuatroDate=$(date -d "$fechaFinMesAnterior-24 month" +%Y-%m-%d)
MesTres=$(date -d "$fechaIniMes-3 month" +%Y%m%d)

# PARAMETROS DEFINIDOS EN LA TABLA params
VAL_CADENA_JDBC=$(mysql -N <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';")
VAL_USER=$(mysql -N <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';")
VAL_COLA_EJECUCION=$(mysql -N <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';")
RUTA=$(mysql -N <<<"select valor from params where entidad = '"$ENTIDAD"' and ambiente='"$AMBIENTE"' AND parametro = 'RUTA';")
ESQUEMA=$(mysql -N <<<"select valor from params where entidad = '"$ENTIDAD"' and ambiente='"$AMBIENTE"' AND parametro = 'ESQUEMA';")
COLA_HIVE=$(mysql -N <<<"select valor from params where entidad = '"$ENTIDAD"' and ambiente='"$AMBIENTE"' AND parametro = 'COLA_HIVE';")
VAL_KINIT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';")
$VAL_KINIT

log_Extraccion=$RUTA/logs/carga_informacion_b2b_$ini_fecha.log
#---------------------------------------------------
# Carga variables de configuracion desde table de parametros
#---------------------------------------------------
echo "****** Carga variables de configuracion ******"
echo "Ultimo dia mes anterior" $fechaFinMesAnterior
echo "fechaIniMes" $fechaIniMes
echo "fechaIniMesDate" $fechaIniMesDate
echo "fechaFinMesAnterior" $fechaFinMesAnterior
echo "fecha_eje1" $fecha_eje1
echo "MesTresDate" $MesTresDate
echo "MesVeinteCuatro" $MesVeinteCuatro
echo "MesVeinteCuatroDate" $MesVeinteCuatroDate
echo "MesTres" $MesTres

# Verificacion de fecha de ejecucion
if [ -z "$FECHAEJE" ]; then # valida que este en blanco el parametro
    echo " $TIME [ERROR] Falta el parametro de fecha de ejecucion del programa"
    exit 2
fi

##############################################
# ETAPA 1: EJECUCION DEL PROCESO DE CARGA B2B
##############################################

#--------------------------------------------------
# Impresion de parametros
#---------------------------------------------------
echo "----------------------------------" >>$log_Extraccion
echo "------*****INICIO ETAPA 1*****--------------" >>$log_Extraccion
echo "COLA:" $COLA_HIVE >>$log_Extraccion
echo "ESQUEMA HIVE: " $ESQUEMA >>$log_Extraccion
echo "DIR SERV: " $RUTA >>$log_Extraccion
echo "FECHA EJECUCION: " $FECHAEJE >>$log_Extraccion
echo "FECHA EJECUCION MENOS 24 MESES : " $MesVeinteCuatro >>$log_Extraccion
echo "FECHA EJECUCION MENOS 3 MESES DATE: " $MesTresDate >>$log_Extraccion
echo "FECHA EJECUCION MENOS 3 MESES : " $MesTres >>$log_Extraccion
echo "****** Fin Carga variables de configuracion ******"
echo "----------------------------------" >>$log_Extraccion

#--------------------------------------------
# Ejecucion del archivo sql
#-------------------------------------------
beeline -u $VAL_CADENA_JDBC -n $VAL_USER \
    --hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
    --hivevar esquema=$ESQUEMA \
    --hivevar FechaProceso=$fechaFinMesAnterior \
    --hivevar mesTres=$MesTres \
    --hivevar mesVeinteCuatro=$MesVeinteCuatro \
    --hivevar mesVeinteCuatroDate=$MesVeinteCuatroDate \
    --hivevar mesTresDate=$MesTresDate \
    -f $RUTA/sql/carga_informacion_b2b.sql &>>$log_Extraccion

# Verificacion de creacion tabla external
if [ $? -eq 0 ]; then
    echo "EXITO en la ETAPA ${ETAPA}" &>>$log_Extraccion
else
    error=3
    ETAPA=1
    echo "ERROR en la ETAPA ${ETAPA}" &>>$log_Extraccion
    exit $error
fi

# seteo de etapa
echo "EXITO ETAPA 1" &>>$log_Extraccion
