with MOVEMENT_ORDERS AS ( --- 5M reg
    SELECT object_id,
        NULL AS CONTEXTID,
        'Dekiting' AS type_,
        LOCATION_FROM,
        LOCATION_TO,
        MOVEMENT_TYPE,
        STATUS
    FROM R_AM_MO_DEKITTING mo_d
    UNION ALL
    SELECT object_id,
        NULL AS CONTEXTID,
        'Kiting' AS type_,
        LOCATION_FROM,
        LOCATION_TO,
        MOVEMENT_TYPE,
        STATUS
    FROM R_AM_MO_KITTING mo_k
    UNION ALL
    SELECT object_id,
        NULL AS CONTEXTID,
        'Purchase' AS type_,
        LOCATION_FROM,
        LOCATION_TO,
        MOVEMENT_TYPE,
        STATUS
    FROM R_AM_MO_PURCHASE mo_p
    UNION ALL
    SELECT object_id,
        NULL AS CONTEXTID,
        'Repair' AS type_,
        LOCATION_FROM,
        LOCATION_TO,
        MOVEMENT_TYPE,
        STATUS
    FROM R_AM_MO_REPAIR mo_rep
    UNION ALL
    SELECT object_id,
        NULL AS CONTEXTID,
        'Return' AS type_,
        LOCATION_FROM,
        LOCATION_TO,
        MOVEMENT_TYPE,
        STATUS
    FROM R_AM_MO_RETURN mo_ret
    UNION ALL
    SELECT object_id,
        CONTEXTID,
        'Sales' AS type_,
        LOCATION_FROM,
        LOCATION_TO,
        MOVEMENT_TYPE,
        STATUS
    FROM R_AM_MO_SALES mo_s
    UNION ALL
    SELECT object_id,
        NULL AS CONTEXTID,
        'Transfer' AS type_,
        LOCATION_FROM,
        LOCATION_TO,
        MOVEMENT_TYPE,
        STATUS
    FROM R_AM_MO_TRANSFER mo_t
),
MO_LINES AS ( -- 6 M de reg
    SELECT object_id,
        name,
        parent_id,
        EQUIPMENT_CONDITION,
        STOCK_ITEM_MODEL,
        APPROVED_QUANTITY,
        PREACTIVATION_TEMPLATE
    FROM R_AM_MOL_DEKITTING
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        EQUIPMENT_CONDITION,
        STOCK_ITEM_MODEL,
        APPROVED_QUANTITY,
        PREACTIVATION_TEMPLATE
    FROM R_AM_MOL_KITTING
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        EQUIPMENT_CONDITION,
        STOCK_ITEM_MODEL,
        APPROVED_QUANTITY,
        PREACTIVATION_TEMPLATE
    FROM R_AM_MOL_PURCHASE
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        EQUIPMENT_CONDITION,
        STOCK_ITEM_MODEL,
        APPROVED_QUANTITY,
        PREACTIVATION_TEMPLATE
    FROM R_AM_MOL_REPAIR
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        EQUIPMENT_CONDITION,
        STOCK_ITEM_MODEL,
        APPROVED_QUANTITY,
        PREACTIVATION_TEMPLATE
    FROM R_AM_MOL_RETURN
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        EQUIPMENT_CONDITION,
        STOCK_ITEM_MODEL,
        APPROVED_QUANTITY,
        PREACTIVATION_TEMPLATE
    FROM R_AM_MOL_SALES
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        EQUIPMENT_CONDITION,
        STOCK_ITEM_MODEL,
        APPROVED_QUANTITY,
        PREACTIVATION_TEMPLATE
    FROM R_AM_MOL_TRANSFER
),
SHIPMENT_ORDERS AS (
    SELECT object_id,
        name,
        parent_id,
        RELATED_MOVE_ORD,
        STATUS
    FROM R_AM_SHO_PURCHASE
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        RELATED_MOVE_ORD,
        STATUS
    FROM R_AM_SHO_REPAIR
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        RELATED_MOVE_ORD,
        STATUS
    FROM R_AM_SHO_RETURN
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        RELATED_MOVE_ORD,
        STATUS
    FROM R_AM_SHO_SALES
    UNION ALL
    SELECT object_id,
        name,
        parent_id,
        RELATED_MOVE_ORD,
        STATUS
    FROM R_AM_SHO_TRANSFER
)
SELECT
	/*+ parallel(8) use_hash(ca omp) use_hash(mo so) use_hash(ssl soi)*/
	'''' || wi.object_id work_item_id,
	';' || to_char(wi.created_when,	'dd/mm/yyyy hh24:mi:ss') || ';' created_when,
	mo.type_ || ';' type_,
	mot.name || ';' mo_type,
	'''' || mo.object_id || ';' movement_order_id,
	mos.value || ';' mo_status,
	'''' || ca.object_id || ';' customer_id,
	ca.name || ';' customer_name,
	'''' || mo.contextid || ';' contextid,
	sot.value || ';' so_type,
	eqc.value || ';' EQUIPMENT_CONDITION,
	'''' || mol.PREACTIVATION_TEMPLATE || ';' PREACTIVATION_TEMPLATE_ID,
	coalesce(so_pt.name, ib_pt.name) || ';' PREACTIVATION_TEMPLATE_NAME,
	coalesce(sim_m.name, cpe_m.name, acc_m.name) || ';' equipment_name,
	'''' || coalesce(sim_m.object_id, cpe_m.object_id, acc_m.object_id) || ';' equipment_id,
	coalesce(sim_m.ARTICLE, cpe_m.ARTICLE, acc_m.ARTICLE) || ';' ARTICLE,
	'''' || mol.object_id || ';' move_order_line_id,
	wh_from.name || ';' location_from_name,
	wh_from.code || ';' location_from_id,
	CASE
		WHEN sot.value = 'Distribution Request' THEN branch.name
		ELSE wh_to.name
	end AS location_to_name,
	';' || CASE
		WHEN sot.value = 'Distribution Request' THEN branch.branch_num
		ELSE wh_to.code
	end AS location_to_id,
	';' || mol.APPROVED_QUANTITY || ';' APPROVED_QUANTITY,
	CASE
		WHEN count(soi.object_id) > 1 THEN nvl(sum(re.RESERVED_QUANTITY), 0)/ count(soi.object_id)
		ELSE nvl(sum(re.RESERVED_QUANTITY), 0)
	end AS reserved_quantity,
	';' || nvl(sum(coalesce(ssl.dispatched_quantity, nssl.dispatched_quantity)), 0) || ';' AS dispatched_quantity,
	nvl(sum(coalesce(ssl.RECEIVED_QUANTITY, nssl.received_quantity)), 0) || ';' AS received_quantity,
	nvl(AVG(mol.APPROVED_QUANTITY) - SUM(
               DECODE(soi.status,
                      9141684386013677572,
                      NVL(COALESCE(ssl.RECEIVED_QUANTITY, nssl.received_quantity), 0),
                      NVL(COALESCE(ssl.dispatched_quantity, nssl.dispatched_quantity), 0))),
	0) || ';' AS to_be_dispatched
FROM (
	SELECT *
	FROM R_WORK_ITEM
	WHERE CREATED_WHEN BETWEEN TO_DATE('$dia_hoy/$mes_hoy/2020 10:30:00', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dia_hoy/$mes_hoy/2020 11:59:59', 'DD/MM/YYYY HH24:MI:SS')
) wi
JOIN MOVEMENT_ORDERS mo 
	ON mo.object_id = wi.target_object
LEFT JOIN nc_list_values mos 
	ON mo.status = mos.list_value_id
LEFT JOIN R_AM_MOVEMENT_TYPE mot 
	ON mot.object_id = mo.MOVEMENT_TYPE
LEFT JOIN R_BOE_SALES_ORD so 
	ON so.object_id = mo.CONTEXTID
LEFT JOIN nc_list_values sot 
	ON so.sales_ord_type = sot.list_value_id
LEFT JOIN R_BOE_ORD_MGMT_PROJECT omp 
	ON omp.object_id = so.parent_id
LEFT JOIN (
	SELECT object_id,
		name
	FROM R_CIM_BSNS_CUST_ACCT
	UNION ALL
	SELECT object_id,
		name
	FROM R_CIM_RES_CUST_ACCT
) ca 
	ON omp.parent_id = ca.object_id
LEFT JOIN MO_LINES mol 
	ON mol.parent_id = mo.object_id
LEFT JOIN R_AM_SIM_MODEL sim_m 
	ON sim_m.object_id = mol.STOCK_ITEM_MODEL
LEFT JOIN R_AM_CPE_MODEL cpe_m 
    ON cpe_m.object_id = mol.STOCK_ITEM_MODEL
LEFT JOIN R_AM_ACCESSOR_MODEL acc_m 
    ON acc_m.object_id = mol.STOCK_ITEM_MODEL
LEFT JOIN R_AM_WAREHOUSE wh_from 
    ON mo.LOCATION_FROM = wh_from.object_id
LEFT JOIN R_AM_WAREHOUSE wh_to 
    ON mo.LOCATION_TO = wh_to.object_id
LEFT JOIN nc_list_values eqc 
    ON eqc.list_value_id = mol.EQUIPMENT_CONDITION
LEFT JOIN R_AM_RESERVATION_ENTRY re 
    ON re.MOVE_ORDER_LINE = mol.object_id
LEFT JOIN SHIPMENT_ORDERS soi 
    ON soi.RELATED_MOVE_ORD = mol.parent_id
LEFT JOIN R_AM_NONSERZD_SHIP_LINE nssl 
    ON soi.object_id = nssl.parent_id
	    AND nssl.STOCK_ITEM_MODEL = mol.STOCK_ITEM_MODEL
LEFT JOIN R_AM_SERZD_SHIP_LINE ssl 
    ON soi.object_id = ssl.parent_id
	    AND ssl.STOCK_ITEM_MODEL = mol.STOCK_ITEM_MODEL
        AND (ssl.preactivation_template = mol.PREACTIVATION_TEMPLATE
            OR (ssl.preactivation_template IS NULL
                AND mol.PREACTIVATION_TEMPLATE IS NULL))
        AND (ssl.EQUIPMENT_CONDITION = mol.EQUIPMENT_CONDITION
            OR (ssl.EQUIPMENT_CONDITION IS NULL
                AND mol.EQUIPMENT_CONDITION IS NULL))
LEFT JOIN R_BOE_SALES_ORD so_pt 
    ON mol.PREACTIVATION_TEMPLATE = so_pt.object_id
LEFT JOIN R_AM_INITIAL_BALANCE ib_pt 
    ON mol.PREACTIVATION_TEMPLATE = ib_pt.object_id
LEFT JOIN R_PMGT_BRNCH branch 
    ON so.branch = branch.object_id
WHERE WI.CREATED_WHEN BETWEEN TO_DATE('$dia_hoy/$mes_hoy/2020 10:30:00', 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE('$dia_hoy/$mes_hoy/2020 11:59:59', 'DD/MM/YYYY HH24:MI:SS')
GROUP BY wi.object_id,
	wi.created_when,
	mo.type_,
	mot.name,
	mo.object_id,
	mos.value,
	ca.object_id,
	ca.name,
	mo.contextid,
	sot.value,
	eqc.value,
	coalesce(sim_m.name, cpe_m.name, acc_m.name),
	coalesce(sim_m.object_id, cpe_m.object_id, acc_m.object_id),
	coalesce(sim_m.ARTICLE, cpe_m.ARTICLE, acc_m.ARTICLE),
	mol.object_id,
	wh_from.name,
	wh_from.code,
	wh_to.name,
	wh_to.code,
	mol.APPROVED_QUANTITY,
	mol.PREACTIVATION_TEMPLATE,
	coalesce(so_pt.name, ib_pt.name),
	branch.branch_num,
	branch.name
