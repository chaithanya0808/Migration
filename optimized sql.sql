WITH PTY AS (
    SELECT 
        PARTY_NUMBER, 
        CASE 
            WHEN X_PROFILE_NARRATIVE = 'MLC' THEN 'Y' 
            ELSE 'N' 
        END AS WEALTH 
    FROM 
        FCFCOR.FSC_PARTY_DIM 
    WHERE 
        CHANGE_CURRENT_IND = 'Y'
),

impacted_cases AS (
    -- these are the cases with wealth customers that still need to be included from SAS to FCEM as per the new requirement
    SELECT 
        A.CASE_RK, 
        COUNT(DISTINCT WEALTH) AS CNT_DIST_WEALTH 
    FROM 
        ECM.CASE_X_PARTY A
    INNER JOIN 
        ECM.PARTY_LIVE B ON A.PARTY_RK = B.PARTY_RK
    INNER JOIN 
        PTY C ON B.PARTY_ID = C.PARTY_NUMBER
    GROUP BY 
        A.CASE_RK
    HAVING 
        COUNT(DISTINCT WEALTH) > 1
),

temp AS (
    SELECT RR_rk, valid_from_dttm 
    FROM ECM.RR_VERSION 
    WHERE RR_SUBCATEGORY_CD = 'SMR' 
      AND ora_rowscn BETWEEN {startscn} AND {endscn}
    
    UNION ALL

    SELECT RR_rk, valid_from_dttm 
    FROM ECM.RR_LIVE 
    WHERE RR_SUBCATEGORY_CD = 'SMR' 
      AND ora_rowscn BETWEEN {startscn} AND {endscn}
    
    UNION ALL

    SELECT 
        ucv.RR_rk, 
        ucv.valid_from_dttm 
    FROM 
        ECM.RR_UDF_LEGFIN_VALUE ucv
    INNER JOIN 
        ECM.RR_VERSION rv ON ucv.RR_rk = rv.RR_rk
    WHERE 
        rv.RR_SUBCATEGORY_CD = 'SMR'
      AND ucv.ora_rowscn BETWEEN {startscn} AND {endscn}
    
    UNION ALL

    SELECT 
        IL.RR_rk, 
        IL.valid_from_dttm 
    FROM 
        ECM.RR_LIVE IL
    INNER JOIN 
        ECWMSD.SA_COMMENT SC 
            ON CAST(IL.RR_rk AS VARCHAR(20)) = SC.OBJECT_ID 
           AND SC.OBJECT_TYPE_ID = 545007
    WHERE 
        IL.RR_SUBCATEGORY_CD = 'SMR' 
      AND SC.ora_rowscn BETWEEN {startscn} AND {endscn}
)

SELECT 
    temp.RR_rk, 
    temp.valid_from_dttm 
FROM 
    temp
WHERE 
    RR_rk NOT IN (
        SELECT DISTINCT RL.RR_rk 
        FROM ECM.RR_LIVE RL
        INNER JOIN ECM.CASE_X_PARTY CXP ON RL.PARENT_OBJECT_RK = CXP.CASE_RK
        INNER JOIN ECM.PARTY_LIVE PL ON CXP.PARTY_RK = PL.PARTY_RK
        INNER JOIN PTY ON PL.PARTY_ID = PTY.PARTY_NUMBER
        WHERE 
            PTY.WEALTH = 'Y'
            AND CXP.CASE_RK IN (
                SELECT CASE_RK FROM impacted_cases
            )
    );
