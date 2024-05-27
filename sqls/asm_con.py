class sql():

  def get_select_msg_payload(project_id):
    return '''
    -- Author: hexton.chan@hkexpress.com 
    -- Description: Get 7-day flight schedule IN which updated BY 7-day before departure, generate ASM_CON
  SELECT
    ID,
    FLTID,
    DATOP_CHN,
    'OpmLegChangeAsmCon-' || FLTID || UPPER(FORMAT_DATETIME("-%y%m%d%H%M%S",SRC_UPDATED_TIME))  AS correlationId,
    'ASM' AS msgType,
    --'UTC' AS timeZone,
    --UPPER(FORMAT_DATETIME("%d%b%y",CURRENT_DATETIME())) AS msgGenTime,
    --'CON' AS msgSubType,
    --FLIGHT_NO,
    --UPPER(FORMAT_DATETIME("%d%b%y",DATOP)) AS DATOP,
    --ACTYPE,
    --LONG_REG,
    --SERVICE_TYPE_ID,
    --DEPSTN,
    --ARRSTN,
    --FORMAT_DATETIME("%d%H%M",STD) AS STD,
    --FORMAT_DATETIME("%d%H%M",STA) AS STA,
    --SRC_CREATED_TIME,
    --SRC_UPDATED_TIME,
    'ASM\\r\\nUTC\\r\\n' ||
    UPPER(FORMAT_DATETIME("%d%b%y",DATOP)) || '001E001\\r\\n' ||
    'CON\\r\\n' ||
    ACOWN||SRC_FLIGHT_NO || '/' || UPPER(FORMAT_DATETIME("%d%b%y",DATOP)) || '\\r\\n' ||
    CASE
      WHEN SERVICE_TYPE_ID = 1 THEN 'C'
      WHEN SERVICE_TYPE_ID = 2 THEN 'D'
      WHEN SERVICE_TYPE_ID = 3 THEN 'E'
      WHEN SERVICE_TYPE_ID = 4 THEN 'F'
      WHEN SERVICE_TYPE_ID = 5 THEN 'J'
      WHEN SERVICE_TYPE_ID = 6 THEN 'K'
      WHEN SERVICE_TYPE_ID = 7 THEN 'L'
      WHEN SERVICE_TYPE_ID = 8 THEN 'M'
      WHEN SERVICE_TYPE_ID = 9 THEN 'O'
      WHEN SERVICE_TYPE_ID = 10 THEN 'P'
      WHEN SERVICE_TYPE_ID = 11 THEN 'Q'
      WHEN SERVICE_TYPE_ID = 12 THEN 'S'
      WHEN SERVICE_TYPE_ID = 13 THEN 'T'
      WHEN SERVICE_TYPE_ID = 14 THEN 'X'
      WHEN SERVICE_TYPE_ID = 15 THEN 'Z'
      WHEN SERVICE_TYPE_ID = 16 THEN 'T'
    ELSE
    'J'
  END
    || ' ' ||
    CASE
      WHEN ACTYPE = 'A321' THEN '321'
      WHEN ACTYPE = 'A320' THEN '320'
      WHEN ACTYPE = 'A21N' THEN '32Q'
    ELSE
    ACTYPE
  END
    || ' . ' || LONG_REG || '\\r\\n' || DEPSTN || FORMAT_DATETIME("%d%H%M",STD) || ' ' || ARRSTN || FORMAT_DATETIME("%d%H%M",STA)|| '\\r\\n' AS payload
  FROM
    `{}.ODS_FlightNet.M_FOC_LEGS`
  WHERE
    -- DateTime Conditions: Next 7-days' flight -- 
    DATE_DIFF(DATOP_CHN,CURRENT_DATE('Asia/Hong_Kong'), DAY) <= 7
    AND DATE(DATOP_CHN) >= CURRENT_DATE('Asia/Hong_Kong') -- (Include Today)
    -- DateTime Conditions -- 
    
    -- Standard Conditions: Identify a vaild flight schedule --
    AND DELETED IS FALSE
    AND long_reg IS NOT NULL
    AND ATA IS NULL
    AND ATD IS NULL
    AND TOFF IS NULL
    AND TDWN IS NULL
    AND SRC_Flight_no IS NOT NULL
    AND FLTID IS NOT NULL
    AND LEGNO IS NOT NULL
    AND DATETIME_DIFF(DATETIME(STA), DATETIME(STD), MINUTE) > 0 
    -- Standard Conditions --
    
    -- ASM_CON Conditions: Catch updates WITHIN 7-days before departure --
    AND SRC_CREATED_TIME <> SRC_UPDATED_TIME
    AND DATE(SRC_UPDATED_TIME) > (DATOP_CHN - 7) 
    -- ASM_CON Conditions --

  ORDER BY
    DATOP ASC

  '''.format(project_id)

  def get_select_msg_history(project_id):
    return '''
  SELECT
    ID,
    FLTID,
    DATOP_CHN,
    all_msg.correlationId,
    msgType,
    payload
  FROM
    `{}.ODS_Eking.ASM_CON` all_msg
  INNER JOIN (
    SELECT
      correlationId,
      MAX( PARSE_DATE('%d%b%y', REPLACE(REGEXP_SUBSTR(payload, '.{{7}}001E001'), '001E001', '' ) ) ) AS latestDate
    FROM
      `{}.ODS_Eking.ASM_CON`
    GROUP BY
      correlationId
    ORDER BY
      latestDate DESC ) latest_msg
  ON
    all_msg.correlationId = latest_msg.correlationId
    --AND payload LIKE CONCAT('%', UPPER(CAST(latest_msg.latestDate AS STRING)), '%')
    AND PARSE_DATE('%d%b%y',  REPLACE(REGEXP_SUBSTR(payload, '.{{7}}001E001'), '001E001', '' ) ) = latest_msg.latestDate
  --TEST: WHERE all_msg.correlationId = 'OpmLegChangeAsmCon-170710-230130131908'
  WHERE
      (DATOP_CHN) >= DATE(CURRENT_DATE('Asia/Hong_Kong'))
  '''.format(project_id, project_id)

  def get_msg_history_table_id(project_id):
    return project_id + '.ODS_Eking.ASM_CON'