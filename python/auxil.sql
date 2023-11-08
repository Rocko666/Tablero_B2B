
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
